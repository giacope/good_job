# frozen_string_literal: true

require 'rails_helper'

RSpec.describe 'Fiber Scheduler Integration', :fiber_isolation, :requires_async do
  let(:adapter) { GoodJob::Adapter.new(execution_mode: :external) }

  before do
    ActiveJob::Base.queue_adapter = adapter
    GoodJob.preserve_job_records = true

    stub_const "RUN_JOBS", Concurrent::Array.new
    stub_const "IN_FLIGHT", Concurrent::AtomicFixnum.new(0)
    stub_const "MAX_IN_FLIGHT", Concurrent::AtomicFixnum.new(0)
  end

  context 'when there are a large number of jobs' do
    let(:number_of_jobs) { 250 }
    let(:fibers) { 10 }

    before do
      stub_const 'TestJob', (Class.new(ActiveJob::Base) do
        self.queue_name = 'test'

        def perform(*_args)
          in_flight = IN_FLIGHT.increment
          MAX_IN_FLIGHT.update { |max| [max, in_flight].max }
          sleep 0.01
          RUN_JOBS << [provider_job_id, Thread.current.name]
        ensure
          IN_FLIGHT.decrement
        end
      end)
    end

    it 'executes each job exactly once, as concurrent fibers on the reactor thread' do
      GoodJob::Job.logger.silence do
        jobs = Array.new(number_of_jobs) { |i| TestJob.new(i) }
        TestJob.queue_adapter.enqueue_all(jobs)
      end

      performer = GoodJob::JobPerformer.new('*')
      scheduler = GoodJob::Scheduler.new(performer, fibers: fibers)
      fibers.times { scheduler.create_thread }

      wait_until(max: 60, increments_of: 0.5) { expect(GoodJob::Job.unfinished.count).to be_zero }
      scheduler.shutdown
      expect(scheduler).to be_shutdown

      expect(RUN_JOBS.size).to eq(number_of_jobs), -> { "Expected every job to run exactly once, but #{RUN_JOBS.size} runs were recorded" }
      expect(RUN_JOBS.map(&:first).uniq.size).to eq number_of_jobs
      expect(RUN_JOBS.map(&:last)).to all(include("reactor"))
      expect(MAX_IN_FLIGHT.value).to be > 1
      expect(GoodJob::Execution.count).to eq number_of_jobs
    end
  end

  context 'when jobs error and retry' do
    before do
      stub_const 'RetryableError', Class.new(StandardError)
      stub_const 'ErrorJob', (Class.new(ActiveJob::Base) do
        self.queue_name = 'test'
        retry_on RetryableError, wait: 0, attempts: 3

        def perform
          RUN_JOBS << provider_job_id
          raise RetryableError if executions < 3
        end
      end), transfer_nested_constants: true
    end

    it 'retries jobs to completion' do
      ErrorJob.perform_later

      performer = GoodJob::JobPerformer.new('*')
      scheduler = GoodJob::Scheduler.new(performer, fibers: 5)
      scheduler.create_thread

      wait_until(max: 10, increments_of: 0.5) { expect(GoodJob::Job.unfinished.count).to be_zero }
      scheduler.shutdown

      expect(RUN_JOBS.size).to eq 3
    end
  end
end
