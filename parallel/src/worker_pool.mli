open! Core
open! Async
open! Import

type ('config, 'info, 'args, 'message) t

val create
  :  (module Worker.S
        with type config = 'config
         and type args = 'args
         and type message = 'message)
  -> ('config * 'info) Nonempty_list.t
  -> ('config, 'info, 'args, 'message) t Or_error.t Deferred.t

val close : _ t -> unit Deferred.t

val create_producer
  :  ('config, 'info, 'args, 'message) t
  -> args:(worker:int -> 'config -> 'info -> 'args)
  -> 'message Iterator.Global.Producer.t Or_error.t Deferred.t

val number_of_workers : _ t -> int
