open! Core
open! Async
open! Import
include Parallel_iterator_intf
include Types

let make : type config args message. (config, args, message) make =
  fun worker ->
  (module struct
    type nonrec config = config
    type nonrec args = args
    type nonrec message = message
    type 'info t = (config, 'info, args, message) Worker_pool.t

    let create workers = Worker_pool.create worker workers
    let close = Worker_pool.close
    let create_producer = Worker_pool.create_producer
    let number_of_workers = Worker_pool.number_of_workers
  end)
;;
