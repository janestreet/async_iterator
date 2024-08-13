open! Core
open! Async
open! Import

module type S = sig
  type config
  type args
  type message
  type t

  val create : config -> t Or_error.t Deferred.t
  val close : t -> unit Deferred.t

  val create_producer
    :  t
    -> args
    -> message Iterator.Global.Producer.t Or_error.t Deferred.t
end

module Types = struct
  type ('config, 'args, 'message) module_ =
    (module S with type config = 'config and type args = 'args and type message = 'message)

  type ('config, 'args, 'message, 'a) make =
    create_producer:('args -> 'a Iterator.Producer.t Or_error.t Deferred.t)
    -> create_consumer:
         ('args
          -> 'message Rpc.Pipe_rpc.Direct_stream_writer.t
          -> 'a Iterator.Consumer.t Or_error.t Deferred.t)
    -> bin_args:'args Bin_prot.Type_class.t
    -> bin_message:'message Bin_prot.Type_class.t
    -> ('config, 'args, 'message) module_
end

module type Worker = sig
  module type S = S (** @inline *)

  include module type of Types (** @inline *)

  module Config : sig
    (** Represents how to spawn a single [Rpc_parallel]-backed worker. *)
    type t =
      { how_to_run : Rpc_parallel.How_to_run.t
      ; name : string
      ; log_dir : string option
      }

    val create
      :  ?how_to_run:Rpc_parallel.How_to_run.t (** default: local *)
      -> ?log_dir:string
      -> name:string
      -> unit
      -> t
  end

  val make : (Config.t, _, _, _) make
  [@@alert
    toplevel
      "[make] depends on [Rpc_parallel.Make], which produces runtime errors if not \
       applied at the toplevel"]

  module For_testing : sig
    val make : (unit, _, _, _) make
  end
end
