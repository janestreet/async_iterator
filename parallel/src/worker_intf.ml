open! Core
open! Async
open! Import

module type S = sig
  type config
  type args
  type message
  type state_update
  type t

  val create : config -> t Or_error.t Deferred.t
  val close : t -> unit Deferred.t
  val create_producer : t -> args -> message Iterator.Producer.t Or_error.t Deferred.t
  val update_state : t -> state_update -> unit Or_error.t Deferred.t
end

module Types = struct
  type ('config, 'args, 'message, 'state_update) module_ =
    (module S
       with type config = 'config
        and type args = 'args
        and type message = 'message
        and type state_update = 'state_update)

  (** The [make] type creates a worker module. The ['state] type parameter represents
      per-worker state that is:
      - Created by [init] when iteration starts
      - Passed to [create_consumer] for use during message processing
      - Accessible to [update_state] for modification (if using state updates)

      The ['state_update] type parameter represents updates sent to workers. Pass
      [~state_updating:Not_using] if you don't need this, which will set ['state_update]
      to [Nothing.t]. *)
  type ('config, 'args, 'message, 'a, 'state, 'state_update) make =
    init:('args -> 'state Or_error.t Deferred.t)
    -> create_producer:('args -> 'a Iterator.Producer.t Or_error.t Deferred.t)
    -> create_consumer:
         ('state
          -> 'args
          -> 'message Rpc.Pipe_rpc.Direct_stream_writer.t
          -> 'a Iterator.Consumer.t Or_error.t Deferred.t)
    -> state_updating:('state, 'state_update) State_updating.t
    -> bin_args:'args Bin_prot.Type_class.t
    -> bin_message:'message Bin_prot.Type_class.t
    -> ('config, 'args, 'message, 'state_update) module_
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

  val make : (Config.t, _, _, _, _, _) make
  [@@alert
    toplevel
      "[make] depends on [Rpc_parallel.Make], which produces runtime errors if not \
       applied at the toplevel"]

  module For_testing : sig
    val make : (unit, _, _, _, _, _) make
  end
end
