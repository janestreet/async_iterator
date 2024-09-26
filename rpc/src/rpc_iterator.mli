open! Core
open! Async
open! Import

(** This module provides the tools necessary to link an iterator on the consumer end of
    a pipe RPC to a data stream on the producer end, respecting [start], [stop], and
    pushback. It is designed for exactly one active iteration per connection. Attempting
    more than one iteration on the same connection will result in an error. *)

module Connection_state : sig
  type t

  val create : unit -> t
end

val start_rpc : ?name:string -> ?version:int -> unit -> unit Rpc.One_way.t

val iter_rpc
  :  ?name:string
  -> ?version:int
  -> bin_args:'args Bin_prot.Type_class.t
  -> bin_message:'message Bin_prot.Type_class.t
  -> unit
  -> ('args, 'message, Error.t) Rpc.Pipe_rpc.t

val stopped_rpc
  :  ?name:string
  -> ?version:int
  -> unit
  -> (unit, unit Or_error.t) Rpc.Rpc.t

val implement_start : Connection_state.t -> unit -> unit

val implement_iter
  :  create_producer:('args -> 'a Iterator.Producer.t Or_error.t Deferred.t)
  -> create_consumer:
       ('args
        -> 'message Rpc.Pipe_rpc.Direct_stream_writer.t
        -> 'a Iterator.Consumer.t Or_error.t Deferred.t)
  -> (Connection_state.t
      -> 'args
      -> 'message Rpc.Pipe_rpc.Direct_stream_writer.t
      -> unit Or_error.t Deferred.t)
       Staged.t

val implement_stopped : Connection_state.t -> unit -> unit Or_error.t Deferred.t

module Global : sig
  (** The underlying [f] is called directly inside of a [Rpc.Pipe_rpc.dispatch_iter]
      handler, so it is possible to do things like bin-read a substring of the
      transport's buffer, as long as the data is copied somewhere before pushback is
      determined. *)
  val of_pipe_rpc
    :  (unit -> Rpc.Connection.t Or_error.t Deferred.t)
    -> 'args
    -> start_rpc:unit Rpc.One_way.t
    -> iter_rpc:('args, 'message, Error.t) Rpc.Pipe_rpc.t
    -> stopped_rpc:(unit, unit Or_error.t) Rpc.Rpc.t
    -> 'message Iterator.Producer.t Or_error.t Deferred.t

  (** [of_direct_pipe] has the same properties w.r.t. access to the RPC transport's
      buffer as [of_pipe_rpc]. *)
  val of_direct_pipe
    :  (module Rpc_parallel.Worker
          with type t = 'worker
           and type connection_state_init_arg = unit)
    -> 'worker
    -> 'args
    -> start_f:('worker, unit, unit) Rpc_parallel.Function.t
    -> iter_f:('worker, 'args, 'message) Rpc_parallel.Function.Direct_pipe.t
    -> stopped_f:('worker, unit, unit Or_error.t) Rpc_parallel.Function.t
    -> 'message Iterator.Producer.t Or_error.t Deferred.t
end
