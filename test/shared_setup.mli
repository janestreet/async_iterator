open! Core
open! Async
open! Import

val%template run_test_for_all_producers
  :  iter:
       (f:('a @ m -> unit Maybe_pushback.t)
        -> stop:unit Deferred.t
        -> unit Or_error.t Deferred.t)
  -> iter':
       (f:('a @ m -> Iterator.Action.t @ local)
        -> stop:unit Deferred.t
        -> unit Or_error.t Deferred.t)
  -> create_consumer:(unit -> ('b Iterator.Consumer.t[@mode m]))
  -> operation:(('a, 'b) Operation.t[@mode m])
  -> unit Deferred.t
[@@mode m = (global, local)]

module Batched : sig
  val run_test_for_all_producers
    :  iter:
         (f:('a Queue.t -> unit Maybe_pushback.t)
          -> stop:unit Deferred.t
          -> unit Or_error.t Deferred.t)
    -> iter':
         (f:('a Queue.t -> Iterator.Action.t @ local)
          -> stop:unit Deferred.t
          -> unit Or_error.t Deferred.t)
    -> create_consumer:(unit -> 'b Iterator.Batched.Consumer.t)
    -> operation:('a, 'b) Operation.t
    -> unit Deferred.t
end
