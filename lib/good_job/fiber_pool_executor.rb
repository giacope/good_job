# frozen_string_literal: true

require "concurrent/atomic/atomic_fixnum"
require "concurrent/executor/executor_service"

module GoodJob # :nodoc:
  #
  # FiberPoolExecutor executes tasks as fibers on a single reactor thread,
  # using the +async+ gem's event loop. It implements the subset of the
  # +Concurrent::ThreadPoolExecutor+ interface that {Scheduler} (and
  # +Concurrent::ScheduledTask+ / +Concurrent::Future+) use, so it can be
  # swapped in wherever the Scheduler would use a thread pool.
  #
  # All fibers share one thread: tasks must be IO-bound to benefit, and
  # Rails' execution state must be fiber-scoped
  # (+config.active_support.isolation_level = :fiber+) so each fiber gets
  # its own Active Record connection.
  #
  # @private
  class FiberPoolExecutor
    include Concurrent::ExecutorService

    # @return [String] name of the executor, used for the reactor thread name
    attr_reader :name

    # @return [Integer] maximum number of concurrently executing fibers
    attr_reader :max_fibers

    # @param max_fibers [Integer] maximum number of concurrently executing fibers
    # @param name [String, nil] name for the reactor thread
    def initialize(max_fibers:, name: nil)
      raise ArgumentError, "max_fibers must be at least 1, but was #{max_fibers.inspect}" unless max_fibers.is_a?(Integer) && max_fibers >= 1

      require "async"
      require "async/semaphore"
      self.class.consume_io_buffer_warning

      @name = name
      @max_fibers = max_fibers
      @queue = ::Thread::Queue.new
      @wakeup_reader, @wakeup_writer = ::IO.pipe
      @pending_count = Concurrent::AtomicFixnum.new(0)
      @mutex = Mutex.new
      @reactor_thread = nil
      @pid = ::Process.pid
    end

    # +IO::Buffer+ emits a one-time "experimental" warning on first use, which
    # would otherwise surface mid-job the first time a fiber performs IO;
    # trigger it here with the warning muted.
    def self.consume_io_buffer_warning
      return if @io_buffer_warning_consumed || !defined?(::IO::Buffer)

      @io_buffer_warning_consumed = true
      original = Warning[:experimental]
      Warning[:experimental] = false
      begin
        ::IO::Buffer.new(1).free
      ensure
        Warning[:experimental] = original
      end
    end

    # Enqueue a task to be executed as a fiber on the reactor thread, which is
    # created lazily for fork safety. Always accepts while running:
    # +Concurrent::TimerSet+ strands delayed tasks when +post+ returns false,
    # so the queue is unbounded and concurrency is bounded by the reactor's
    # semaphore instead.
    # @return [Boolean] whether the task was accepted
    def post(*args, &block)
      return false if @queue.closed?

      # Synchronized with the crash-recovery reset in #run_reactor: the
      # increment and push must land atomically, and a reactor observed alive
      # here either pops the task or respawns a replacement in its ensure.
      accepted = @mutex.synchronize do
        reset_after_fork if @pid != ::Process.pid
        @pending_count.increment
        begin
          @queue.push([args, block])
        rescue ClosedQueueError
          @pending_count.decrement
          next false
        end
        spawn_reactor unless @reactor_thread&.alive?
        true
      end
      wake_reactor if accepted
      accepted
    end

    # @return [Boolean] whether the executor is accepting new tasks
    def running?
      !@queue.closed?
    end

    # @return [Boolean] whether the executor is stopping but still executing tasks
    def shuttingdown?
      @queue.closed? && reactor_alive?
    end

    # @return [Boolean] whether the executor has fully stopped
    def shutdown?
      @queue.closed? && !reactor_alive?
    end

    # Stop accepting new tasks; already-enqueued tasks will complete.
    # @return [void]
    def shutdown
      @queue.close
      thread = @mutex.synchronize do
        thread = @reactor_thread
        close_wakeup_pipe unless thread&.alive?
        thread
      end
      wake_reactor if thread&.alive?
    end

    # Stop accepting new tasks, discard enqueued tasks, and kill the reactor.
    # @return [void]
    def kill
      @queue.close
      @queue.clear
      thread = @mutex.synchronize do
        thread = @reactor_thread
        close_wakeup_pipe unless thread&.alive?
        thread
      end
      thread&.kill
    end

    # Block until in-progress and enqueued tasks have finished.
    # @param timeout [Numeric, nil] seconds to wait, or +nil+ to wait forever
    # @return [Boolean] whether the executor fully stopped
    def wait_for_termination(timeout = nil)
      thread = @mutex.synchronize { @reactor_thread }
      return true if thread.nil? || !thread.alive?

      !thread.join(timeout).nil?
    end

    # Number of additional tasks that can be executed concurrently, counting
    # enqueued-but-not-started tasks against capacity.
    # @return [Integer]
    def ready_worker_count
      count = @max_fibers - @pending_count.value
      count.positive? ? count : 0
    end

    # Run a callback after the current task releases its fiber capacity.
    # +Concurrent::ScheduledTask+ notifies observers before its executor block
    # returns, so an observer that creates the next task must defer it to see
    # the completed fiber as available.
    # @return [Boolean] whether the callback was deferred
    def defer_after_current_task(&block)
      return false unless block

      callbacks = Thread.current[:good_job_fiber_pool_executor_callbacks]
      return false unless callbacks

      callbacks << block
      true
    end

    private

    # Exceptions that must not be contained: Async's task teardown signals,
    # which are how the reactor stops a fiber, and process-level signals.
    # Resolved lazily because +async+ is required in the constructor.
    # @return [Array<Class>]
    def fatal_exceptions
      @_fatal_exceptions ||= [
        (::Async::Stop if defined?(::Async::Stop)),
        (::Async::Cancel if defined?(::Async::Cancel)),
        ::SystemExit,
        ::SignalException,
      ].compact
    end

    def reactor_alive?
      thread = @mutex.synchronize { @reactor_thread }
      !thread.nil? && thread.alive?
    end

    # A forked child inherits the queue and pending count but not the reactor
    # thread; reset so it neither double-executes inherited tasks nor leaks
    # capacity. Must be called while holding +@mutex+.
    def reset_after_fork
      @pid = ::Process.pid
      @queue.clear
      @pending_count.value = 0
      @reactor_thread = nil
      # Replace the pipe shared with the parent's reactor.
      close_wakeup_pipe
      @wakeup_reader, @wakeup_writer = ::IO.pipe
    end

    # Must be called while holding +@mutex+.
    def spawn_reactor
      @wakeup_reader, @wakeup_writer = ::IO.pipe if @wakeup_reader.closed? || @wakeup_writer.closed?
      @reactor_thread = ::Thread.new { run_reactor }
    end

    # Signal the reactor via the wakeup pipe. A full buffer already implies a
    # pending wakeup; a closed pipe means the reactor no longer needs waking.
    def wake_reactor
      @wakeup_writer.write_nonblock("!")
    rescue IO::WaitWritable, IOError, Errno::EPIPE
      nil
    end

    # Drains queued tasks and spawns each as a fiber, bounded by a semaphore.
    # Waits for work on the wakeup pipe rather than +Thread::Queue#pop+
    # because IO waits are fiber-scheduler aware, so only the waiting fiber
    # suspends while in-flight job fibers keep running.
    def run_reactor
      ::Thread.current.name = "#{name}-reactor"
      reader = @wakeup_reader
      read_buffer = ::String.new

      Async do |reactor|
        semaphore = Async::Semaphore.new(@max_fibers, parent: reactor)

        loop do
          while (item = pop_nonblock)
            args, block = item
            semaphore.async do
              callbacks = []
              Thread.current[:good_job_fiber_pool_executor_callbacks] = callbacks
              block.call(*args)
            rescue *fatal_exceptions
              # Reactor teardown and process signals must reach the reactor.
              raise
            rescue Exception => e # rubocop:disable Lint/RescueException
              # Contain the error like a thread pool does: unhandled, it would
              # stop the reactor and cancel unrelated in-flight fibers. A thread
              # pool loses one worker to a non-StandardError; the reactor would
              # lose every job running at that moment, and they would be
              # re-executed on the respawn.
              GoodJob._on_thread_error(e)
            ensure
              @pending_count.decrement
              callbacks&.each do |callback|
                callback.call
              rescue StandardError => e
                GoodJob._on_thread_error(e)
              end
            end
          end

          break if @queue.closed?

          reader.wait_readable
          begin
            reader.read_nonblock(4096, read_buffer) # consume coalesced wakeups
          rescue IO::WaitReadable
            nil
          rescue EOFError
            break
          end
        end
      end
    rescue *fatal_exceptions
      raise
    rescue Exception => e # rubocop:disable Lint/RescueException
      GoodJob._on_thread_error(e)
    ensure
      # A crashed reactor never ran its in-flight fibers' decrements: resync
      # the count to the queued backlog, and respawn if tasks were pushed
      # while this thread was dying (#post saw it alive and did not spawn).
      @mutex.synchronize do
        @pending_count.value = @queue.size
        @reactor_thread = nil if @reactor_thread == ::Thread.current
        if !@queue.closed? && !@queue.empty? && @reactor_thread.nil?
          spawn_reactor
        elsif @reactor_thread.nil?
          close_wakeup_pipe(reader, @wakeup_writer)
        end
      end
    end

    def close_wakeup_pipe(reader = @wakeup_reader, writer = @wakeup_writer)
      [reader, writer].each do |io|
        io.close unless io.closed?
      rescue IOError
        nil
      end
    end

    def pop_nonblock
      return if @queue.empty?

      @queue.pop(true)
    rescue ThreadError
      nil
    end
  end
end
