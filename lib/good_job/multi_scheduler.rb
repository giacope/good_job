# frozen_string_literal: true

module GoodJob
  # Delegates the interface of a single {Scheduler} to multiple Schedulers.
  class MultiScheduler
    # Creates MultiScheduler from a GoodJob::Configuration instance.
    # @param configuration [GoodJob::Configuration]
    # @param warm_cache_on_initialize [Boolean]
    # @return [GoodJob::MultiScheduler]
    def self.from_configuration(configuration, capsule: GoodJob.capsule, warm_cache_on_initialize: false)
      # A malformed per-queue pool size is a hard error in every mode, so
      # parse before the fiber fallback rather than inside it.
      queue_configurations = configuration.queue_string.split(';').map(&:strip).map do |queue_string_and_count|
        queue_string, queue_count = queue_string_and_count.split(':').map { |str| str.strip.presence }

        if queue_count
          pool_size = Integer(queue_count, 10, exception: false)
          raise ArgumentError, "GoodJob queue pool size must be a positive integer, but was '#{queue_count}' for queue '#{queue_string}'" unless pool_size&.positive?

          queue_count = pool_size
        end

        [queue_string, queue_count]
      end

      fibers_fallback = false
      begin
        fibers = configuration.fibers

        if fibers
          Scheduler.validate_fiber_execution!

          GoodJob.logger.warn("GoodJob: `fibers` (#{fibers}) takes precedence over the configured `max_threads` (#{configuration.max_threads}); the fiber count sets each scheduler's concurrency.") if configuration.max_threads_configured?
          GoodJob.logger.warn("GoodJob: fiber execution with the :advisory lock strategy holds one database connection per in-flight job. Consider removing the explicit `lock_strategy = :advisory` setting so fibers default to :skiplocked (requires the v4 `lock_type` migration) and can share a small connection pool.") if configuration.lock_strategy == :advisory
          GoodJob.logger.warn("GoodJob: `lower_thread_priority` is ignored with fiber execution because all jobs share a single reactor thread.") if configuration.lower_thread_priority
          warn_downgraded_lock_strategy(configuration)
        end
      rescue ArgumentError => e
        # The CLI worker fails loudly, but a web process must not crash at
        # boot because GOOD_JOB_FIBERS was set for the worker in a shared
        # environment; it falls back to the thread pool.
        raise if configuration.execution_mode == :external

        GoodJob.logger.error("GoodJob: ignoring `fibers` and using a thread pool for this #{configuration.execution_mode} process: #{e.message}")
        fibers = nil
        fibers_fallback = true
      end

      schedulers = queue_configurations.map do |queue_string, queue_count|
        scheduler_options = {
          max_cache: configuration.max_cache,
          warm_cache_on_initialize: warm_cache_on_initialize,
          cleanup_interval_seconds: configuration.cleanup_interval_seconds,
          cleanup_interval_jobs: configuration.cleanup_interval_jobs,
          lower_thread_priority: configuration.lower_thread_priority,
        }

        if fibers
          fiber_count = queue_count || fibers
          GoodJob.logger.warn("GoodJob: the pool size of queue '#{queue_string}' (#{fiber_count}) takes precedence over `fibers` (#{fibers}) and sets that scheduler's fiber count.") if fiber_count != fibers
          scheduler_options[:fibers] = fiber_count
        else
          # On fiber fallback, per-queue sizes were tuned as fiber counts: cap
          # them at max_threads while honoring smaller caps like `serial:1`.
          thread_count = queue_count || configuration.max_threads
          thread_count = [thread_count, configuration.max_threads].min if fibers_fallback
          scheduler_options[:max_threads] = thread_count
        end

        job_performer = GoodJob::JobPerformer.new(queue_string, capsule: capsule)
        GoodJob::Scheduler.new(job_performer, **scheduler_options)
      end

      new(schedulers)
    end

    # Warns when the configured lock strategy is not the one that will actually
    # be used. {Job.effective_lock_strategy} falls back to +:advisory+ when the
    # v4 +lock_type+ column is absent, which silently erases the point of fiber
    # execution: +:advisory+ holds a connection for each job's whole duration,
    # so concurrency is capped at the connection pool size no matter how many
    # fibers are configured. Skipped when the database is unreachable, since
    # this runs at boot.
    # @param configuration [GoodJob::Configuration]
    # @return [void]
    def self.warn_downgraded_lock_strategy(configuration)
      configured = configuration.lock_strategy
      effective = GoodJob::Job.effective_lock_strategy(configured)
      return if effective == configured

      GoodJob.logger.warn(
        "GoodJob: the #{configured.inspect} lock strategy is unavailable and has been downgraded to #{effective.inspect}, " \
        "which holds a database connection for the entire duration of every job — fiber concurrency will be capped at the " \
        "connection pool size. Run `bin/rails g good_job:update` and migrate to add the `good_jobs.lock_type` column."
      )
    rescue StandardError => e
      GoodJob.logger.debug { "GoodJob: could not verify the effective lock strategy: #{e.message}" }
    end
    private_class_method :warn_downgraded_lock_strategy

    # @return [Array<Scheduler>] List of the scheduler delegates
    attr_reader :schedulers

    # @param schedulers [Array<Scheduler>]
    def initialize(schedulers)
      @schedulers = schedulers
    end

    # Delegates to {Scheduler#running?}.
    # @return [Boolean, nil]
    def running?
      schedulers.all?(&:running?)
    end

    # Delegates to {Scheduler#shutdown?}.
    # @return [Boolean, nil]
    def shutdown?
      schedulers.all?(&:shutdown?)
    end

    # Delegates to {Scheduler#shutdown}.
    # @param timeout [Numeric, nil]
    # @return [void]
    def shutdown(timeout: -1)
      GoodJob._shutdown_all(schedulers, timeout: timeout)
    end

    # Delegates to {Scheduler#restart}.
    # @param timeout [Numeric, nil]
    # @return [void]
    def restart(timeout: -1)
      GoodJob._shutdown_all(schedulers, :restart, timeout: timeout)
    end

    # Delegates to {Scheduler#create_thread}.
    # @param state [Hash]
    # @return [Boolean, nil]
    def create_thread(state = nil)
      results = []

      if state && !state[:fanout]
        schedulers.any? do |scheduler|
          scheduler.create_thread(state).tap { |result| results << result }
        end
      else
        schedulers.each do |scheduler|
          results << scheduler.create_thread(state)
        end
      end

      if results.any?
        true
      elsif results.any?(false)
        false
      else # rubocop:disable Style/EmptyElse
        nil
      end
    end

    def lower_thread_priority=(value)
      schedulers.each do |scheduler|
        scheduler.lower_thread_priority = value
      end
    end

    def stats
      scheduler_stats = schedulers.map(&:stats)

      {
        schedulers: scheduler_stats,
        empty_executions_count: scheduler_stats.sum { |stats| stats.fetch(:empty_executions_count, 0) },
        errored_executions_count: scheduler_stats.sum { |stats| stats.fetch(:errored_executions_count, 0) },
        succeeded_executions_count: scheduler_stats.sum { |stats| stats.fetch(:succeeded_executions_count, 0) },
        total_executions_count: scheduler_stats.sum { |stats| stats.fetch(:total_executions_count, 0) },
        execution_at: scheduler_stats.map { |stats| stats.fetch(:execution_at, nil) }.compact.max,
        active_execution_thread_count: scheduler_stats.sum { |stats| stats.fetch(:active_fibers, stats.fetch(:active_threads, 0)) },
        check_queue_at: scheduler_stats.map { |stats| stats.fetch(:check_queue_at, nil) }.compact.max,
      }
    end
  end
end
