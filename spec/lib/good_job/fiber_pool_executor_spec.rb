# frozen_string_literal: true

require 'rails_helper'

RSpec.describe GoodJob::FiberPoolExecutor, :requires_async do
  let(:executor) { described_class.new(max_fibers: 5, name: "test-executor") }

  after do
    executor.kill unless executor.shutdown?
    executor.wait_for_termination(5)
  end

  describe '#post' do
    it 'executes tasks and passes arguments' do
      results = Concurrent::Array.new
      expect(executor.post(1, 2) { |a, b| results << (a + b) }).to be true
      wait_until { expect(results).to eq [3] }
    end

    it 'executes tasks concurrently as fibers' do
      results = Concurrent::Array.new
      monotonic_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      10.times do |i|
        executor.post(i) do |n|
          sleep(0.2)
          results << n
        end
      end
      wait_until(max: 5) { expect(results.size).to eq 10 }
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - monotonic_start

      # 10 tasks x 0.2s at 5 fibers is ~0.4s when interleaved, 2s when serial
      expect(elapsed).to be < 1.5
    end

    it 'completes sleeping fibers while the reactor is idle waiting for work' do
      # Regression: waiting for work must suspend only a fiber, not the
      # reactor thread. A thread-blocking wait (e.g. a non-scheduler-aware
      # Thread::Queue#pop) would prevent this task's sleep from ever resuming.
      completed = Concurrent::AtomicBoolean.new(false)
      executor.post do
        sleep(0.2)
        completed.make_true
      end

      wait_until { expect(completed.true?).to be true }
    end

    it 'returns false after shutdown' do
      executor.shutdown
      expect(executor.post { nil }).to be false
    end

    it 'isolates GoodJob::CurrentThread state between concurrent fibers', :fiber_isolation do
      # CurrentThread's thread_mattr_accessor is backed by
      # ActiveSupport::IsolatedExecutionState, which is fiber-scoped only under
      # `isolation_level = :fiber` — the setting validate_fiber_execution!
      # enforces. This pins that jobs running concurrently on the one reactor
      # thread cannot observe or overwrite each other's execution state.
      results = Concurrent::Hash.new
      2.times do |i|
        executor.post(i) do |n|
          GoodJob::CurrentThread.active_job = "job-#{n}"
          GoodJob::CurrentThread.retry_now = n.zero?
          sleep(0.2) # fiber-scheduler aware; yields to the other fiber mid-job
          results[n] = [GoodJob::CurrentThread.active_job, GoodJob::CurrentThread.retry_now]
        end
      end

      wait_until { expect(results.size).to eq 2 }
      expect(results[0]).to eq ["job-0", true]
      expect(results[1]).to eq ["job-1", false]
    end

    it 'isolates a raising task so other fibers and the reactor survive' do
      errors = Concurrent::Array.new
      allow(GoodJob).to receive(:_on_thread_error) { |error| errors << error }

      results = Concurrent::Array.new
      executor.post { raise "boom" }
      3.times do |i|
        executor.post(i) do |n|
          sleep(0.2)
          results << n
        end
      end

      wait_until { expect(results.size).to eq 3 }
      expect(errors.map(&:message)).to eq ["boom"]
      expect(executor.running?).to be true
      wait_until { expect(executor.ready_worker_count).to eq 5 }
      expect(executor.post { results << :after }).to be true
      wait_until { expect(results).to include :after }
    end

    it 'isolates a task raising a non-StandardError so in-flight fibers are not cancelled' do
      # A thread pool loses one worker to a non-StandardError. Uncontained on a
      # reactor it would stop every fiber mid-flight, and GoodJob would
      # re-execute those jobs after the respawn.
      errors = Concurrent::Array.new
      allow(GoodJob).to receive(:_on_thread_error) { |error| errors << error }

      results = Concurrent::Array.new
      3.times do |i|
        executor.post(i) do |n|
          sleep(0.2)
          results << n
        end
      end
      executor.post { raise Exception, "fatal-ish" } # rubocop:disable Lint/RaiseException

      wait_until { expect(results.size).to eq 3 }
      expect(results).to contain_exactly(0, 1, 2)
      expect(errors.map(&:message)).to eq ["fatal-ish"]
      expect(executor.running?).to be true
      expect(executor.post { results << :after }).to be true
      wait_until { expect(results).to include :after }
    end

    it 'lets Async teardown signals through so the reactor can stop a fiber' do
      expect(executor.send(:fatal_exceptions)).to include(Async::Stop, SystemExit, SignalException)
    end

    it 'accepts tasks beyond capacity and eventually executes them all' do
      # TimerSet dispatches delayed ScheduledTasks via #post and strands them
      # if it returns false, so saturation must queue rather than reject.
      latch = Concurrent::CountDownLatch.new(1)
      completed = Concurrent::AtomicFixnum.new(0)
      accepted = 20.times.count do
        executor.post do
          latch.wait(5)
          completed.increment
        end
      end

      expect(accepted).to eq 20
      expect(executor.ready_worker_count).to eq 0
      latch.count_down
      wait_until { expect(completed.value).to eq 20 }
      wait_until { expect(executor.ready_worker_count).to eq 5 }
    end

    it 'runs deferred callbacks after releasing the completed fiber capacity' do
      available_workers = Concurrent::AtomicFixnum.new(0)

      executor.post do
        expect(executor.defer_after_current_task { available_workers.value = executor.ready_worker_count }).to be true
      end

      wait_until { expect(available_workers.value).to eq 5 }
    end
  end

  describe '#initialize' do
    it 'raises when max_fibers is less than 1' do
      expect { described_class.new(max_fibers: 0) }.to raise_error(ArgumentError, /max_fibers/)
    end
  end

  describe '#ready_worker_count' do
    it 'reflects pending tasks' do
      expect(executor.ready_worker_count).to eq 5

      latch = Concurrent::CountDownLatch.new(1)
      3.times { executor.post { latch.wait(5) } }
      wait_until { expect(executor.ready_worker_count).to eq 2 }

      latch.count_down
      wait_until { expect(executor.ready_worker_count).to eq 5 }
    end
  end

  describe 'reactor crash recovery' do
    it 'resynchronizes capacity accounting after the reactor dies with fibers in flight' do
      started = Concurrent::CountDownLatch.new(5)
      blocker = Concurrent::CountDownLatch.new(1)
      5.times do
        executor.post do
          started.count_down
          blocker.wait(5)
        end
      end
      expect(started.wait(5)).to be true
      expect(executor.ready_worker_count).to eq 0

      # Simulate a crash: in-flight fibers die without running their decrements.
      reactor = executor.instance_variable_get(:@reactor_thread)
      reactor.kill
      reactor.join

      expect(executor.ready_worker_count).to eq 5

      completed = Concurrent::AtomicBoolean.new(false)
      expect(executor.post { completed.make_true }).to be true
      wait_until { expect(completed.true?).to be true }
    end

    it 'does not lose capacity when posts race a reactor crash' do
      posters = Array.new(4) do
        Thread.new { 50.times { executor.post { sleep 0.005 } } }
      end
      3.times do
        sleep 0.01
        executor.instance_variable_get(:@reactor_thread)&.kill
      end
      posters.each(&:join)
      executor.post { nil } # respawn a reactor to drain any backlog

      wait_until(max: 10) { expect(executor.ready_worker_count).to eq 5 }
    end
  end

  describe '#shutdown' do
    it 'drains in-flight tasks and stops the reactor' do
      wakeup_ios = [
        executor.instance_variable_get(:@wakeup_reader),
        executor.instance_variable_get(:@wakeup_writer),
      ]
      completed = Concurrent::AtomicBoolean.new(false)
      executor.post do
        sleep(0.2)
        completed.make_true
      end
      sleep 0.05
      executor.shutdown

      expect(executor.running?).to be false
      expect(executor.wait_for_termination(5)).to be true
      expect(executor.shutdown?).to be true
      expect(completed.true?).to be true
      expect(wakeup_ios).to all(be_closed)
    end

    it 'is immediately shutdown when never started' do
      wakeup_ios = [
        executor.instance_variable_get(:@wakeup_reader),
        executor.instance_variable_get(:@wakeup_writer),
      ]

      executor.shutdown

      expect(executor.shutdown?).to be true
      expect(executor.wait_for_termination(1)).to be true
      expect(wakeup_ios).to all(be_closed)
    end
  end

  describe '#kill' do
    it 'stops the reactor without waiting for tasks' do
      wakeup_ios = [
        executor.instance_variable_get(:@wakeup_reader),
        executor.instance_variable_get(:@wakeup_writer),
      ]
      executor.post { sleep(60) }
      sleep 0.1
      executor.kill
      expect(executor.wait_for_termination(5)).to be true
      expect(executor.shutdown?).to be true
      expect(wakeup_ios).to all(be_closed)
    end
  end
end
