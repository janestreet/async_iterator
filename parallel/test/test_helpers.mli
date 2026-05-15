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

val iter_with
  :  ?create_producer:
       (Args.t -> Message.t Iterator.Batched.Producer.t Or_error.t Deferred.t)
  -> ?bin_message:Message.t Bin_prot.Type_class.t
  -> ?info:(int -> Info.t)
  -> Message.t Iterator.Consumer.t
  -> unit Deferred.t
