open! Core
open! Async
open! Import

type ('config, 'info, 'args, 'message, 'state_update) t

val create
  :  (module Worker.S
        with type config = 'config
         and type args = 'args
         and type message = 'message
         and type state_update = 'state_update)
  -> ('config * 'info) Nonempty_list.t
  -> ('config, 'info, 'args, 'message, 'state_update) t Or_error.t Deferred.t

val close : _ t -> unit Deferred.t

val create_producer
  :  ('config, 'info, 'args, 'message, 'state_update) t
  -> args:(worker:int -> 'config -> 'info -> 'args)
  -> 'message Iterator.Producer.t Or_error.t Deferred.t

(** Returns an error if the worker index is out of bounds or the worker state is not
    initialized yet. *)
val update_worker_state
  :  ('config, 'info, 'args, 'message, 'state_update) t
  -> worker:int
  -> 'state_update
  -> unit Or_error.t Deferred.t

val number_of_workers : _ t -> int
