module Parallel_iterator = Parallel_iterator
module Worker = Worker

module For_testing = struct
  module Worker = struct
    include (Worker : module type of Worker with module For_testing := Worker.For_testing)
    include Worker.For_testing
  end

  module Worker_pool = Worker_pool
end
