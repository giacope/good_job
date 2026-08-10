# frozen_string_literal: true

RSpec.configure do |c|
  unless GoodJob::Scheduler.fiber_execution_supported?
    puts "Excluding fiber specs because this Ruby or async version does not support fiber execution"
    c.filter_run_excluding :requires_async
  end

  if defined?(ActiveSupport::IsolatedExecutionState)
    # Fiber-scoped execution state, as validate_fiber_execution! requires.
    c.around(:example, :fiber_isolation) do |example|
      original_isolation_level = ActiveSupport::IsolatedExecutionState.isolation_level
      ActiveSupport::IsolatedExecutionState.isolation_level = :fiber
      example.run
    ensure
      ActiveSupport::IsolatedExecutionState.isolation_level = original_isolation_level
    end
  else
    puts "Excluding fiber-isolation specs because this Rails version does not support fiber-scoped execution state"
    c.filter_run_excluding :fiber_isolation
  end
end
