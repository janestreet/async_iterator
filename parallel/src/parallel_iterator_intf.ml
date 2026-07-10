open! Core
open! Async
open! Import

module type S = sig
  type config
  type args
  type message
  type state_update
  type 'info t

  val create : (config * 'info) Nonempty_list.t -> 'info t Or_error.t Deferred.t
  val close : _ t -> unit Deferred.t

  val create_producer
    :  'info t
    -> args:(worker:int -> config -> 'info -> args)
    -> message Iterator.Producer.t Or_error.t Deferred.t

  val update_worker_state
    :  _ t
    -> worker:int
    -> state_update
    -> unit Or_error.t Deferred.t

  val number_of_workers : _ t -> int
end

module Types = struct
  type ('config, 'args, 'message, 'state_update) module_ =
    (module S
       with type config = 'config
        and type args = 'args
        and type message = 'message
        and type state_update = 'state_update)

  type ('config, 'args, 'message, 'state_update) make =
    ('config, 'args, 'message, 'state_update) Worker.module_
    -> ('config, 'args, 'message, 'state_update) module_
end

module type Parallel_iterator = sig
  module type S = S

  include module type of Types (** @inline *)

  val make : (_, _, _, _) make
end
