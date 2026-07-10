open! Core
open! Async
open! Import
include Parallel_iterator_intf
include Types

let make
  : type config args message state_update. (config, args, message, state_update) make
  =
  fun worker ->
  (module struct
    type nonrec config = config
    type nonrec args = args
    type nonrec message = message
    type nonrec state_update = state_update
    type 'info t = (config, 'info, args, message, state_update) Worker_pool.t

    let create workers = Worker_pool.create worker workers
    let close = Worker_pool.close
    let create_producer = Worker_pool.create_producer
    let update_worker_state = Worker_pool.update_worker_state
    let number_of_workers = Worker_pool.number_of_workers
  end)
;;
