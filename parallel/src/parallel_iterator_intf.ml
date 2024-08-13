open! Core
open! Async
open! Import

module type S = sig
  type config
  type args
  type message
  type 'info t

  val create : (config * 'info) Nonempty_list.t -> 'info t Or_error.t Deferred.t
  val close : _ t -> unit Deferred.t

  val create_producer
    :  'info t
    -> args:(worker:int -> config -> 'info -> args)
    -> message Iterator.Global.Producer.t Or_error.t Deferred.t

  val number_of_workers : _ t -> int
end

module Types = struct
  type ('config, 'args, 'message) module_ =
    (module S with type config = 'config and type args = 'args and type message = 'message)

  type ('config, 'args, 'message) make =
    ('config, 'args, 'message) Worker.module_ -> ('config, 'args, 'message) module_
end

module type Parallel_iterator = sig
  module type S = S

  include module type of Types (** @inline *)

  val make : (_, _, _) make
end
