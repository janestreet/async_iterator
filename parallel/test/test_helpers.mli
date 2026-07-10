open! Core
open! Async
open! Import

module Info : sig
  type t [@@deriving bin_io]

  val create : ?stop:int -> ?size_budget:int -> unit -> t
end

module Args : sig
  type t =
    { info : Info.t
    ; worker : int
    }
  [@@deriving bin_io]
end

module Message : sig
  type t =
    { worker : int
    ; i : int
    }
  [@@deriving bin_io, equal, sexp_of]
end

val create_producer
  :  Args.t
  -> Message.t Iterator.Batched.Producer.t Or_error.t Deferred.t

val consumer
  :  ?stop_after:Message.t
  -> ?pushback:(Message.t -> unit Maybe_pushback.t)
  -> unit
  -> Message.t Iterator.Consumer.t

val number_of_workers : int

val with_worker_pool
  :  ('config, Args.t, Message.t, 'state_update) Worker.module_
  -> ('config * Info.t) Nonempty_list.t
  -> f:
       (('config, Info.t, Args.t, Message.t, 'state_update) Worker_pool.t
        -> unit Deferred.t)
  -> unit Deferred.t

val default_workers : ?stop:int -> unit -> (unit * Info.t) Nonempty_list.t

val create_producer_from_worker_pool
  :  (_, Info.t, Args.t, Message.t, _) Worker_pool.t
  -> Message.t Iterator.Producer.t Deferred.t

val iter_with
  :  ?create_producer:
       (Args.t -> Message.t Iterator.Batched.Producer.t Or_error.t Deferred.t)
  -> ?bin_message:Message.t Bin_prot.Type_class.t
  -> ?info:(int -> Info.t)
  -> Message.t Iterator.Consumer.t
  -> unit Deferred.t

module Worker_state : sig
  type t = { mutable offset : int }
end

(** Creates a worker with offset-based state. The consumer adds the state offset to each
    message's [i] field, making state updates visible in the output. If [produce_messages]
    isn't specified, it defaults to 0..stop. *)
val make_state_worker
  :  ?init:(Args.t -> Worker_state.t Or_error.t Deferred.t)
  -> ?create_producer:
       (Args.t -> Message.t Iterator.Batched.Producer.t Or_error.t Deferred.t)
  -> unit
  -> (unit, Args.t, Message.t, int) Worker.module_
