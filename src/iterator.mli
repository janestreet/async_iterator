open! Core
open! Async
open! Import

module Action : sig
  type t =
    | Stop
    | Continue
    | Wait of { global_ pushback : unit Deferred.t }
  [@@deriving globalize, sexp_of]
end

type%template -'a f := 'a @ m -> unit Maybe_pushback.t [@@mode m = (global, local)]
type%template -'a f' := 'a @ m -> Action.t @ local [@@mode m = (global, local)]

(** A [Producer.t] represents the input of an iteration. For example, one might want to
    read from a pipe, or from a database, or from a value generator. Generalizing over
    these sources allows us to "connect" them to consumers.

    [Producer.t] and the functions which operate on it are analogous to [Pipe.Reader.t]
    and related functions in the [Pipe] module. *)
module Producer : sig
  type -'f t'

  type%template +'a t = ('a f'[@mode m]) t' [@@mode m = (global, local)]
end

(** A [Consumer.t] represents the output of an iteration. For example, one might want to
    write to a pipe, or to a database, or do some cheap synchronous operation.
    Generalizing over these sinks allows us to "connect" them to producers.

    [Consumer.t] and the functions which operate on it are analogous to [Pipe.Writer.t]
    and related functions in the [Pipe] module. *)
module Consumer : sig
  type +'f t'

  type%template -'a t = ('a f'[@mode m]) t' [@@mode m = (global, local)]
end

[%%template:
[@@@mode.default m = (global, local)]

(** [create_producer] describes how to iterate over some data source, passing each message
    to a supplied consumer. The resulting [unit Or_error.t Deferred.t] should be
    determined when the iteration has stopped.

    [start (create_producer ~iter) (create_consumer ~f ~stop ())] effectively results in
    the same behavior as calling [iter ~f ~stop] directly.

    Like [Pipe.iter], producers are not designed to be [start]ed more than once. The
    behavior in this scenario is left unspecified. However, a good rule of thumb is that
    [stop]ping should cause [iter] to clean up any resources (e.g. calling
    [Pipe.close_read]), and several implementations in this module simply raise if the
    same producer is started multiple times. *)
val create_producer
  :  iter:(f:('a f[@mode m]) -> stop:unit Deferred.t -> unit Or_error.t Deferred.t)
  -> ('a Producer.t[@mode m])

(** [create_producer'] is like [create_producer], but exposes more precise control over
    stopping behavior. *)
val create_producer'
  :  iter:
       (f:('a f'[@mode m])
        -> stop:unit Deferred.Choice.t list
        -> unit Or_error.t Deferred.t)
  -> ('a Producer.t[@mode m])

(** While some producers such as [Pipe.Reader.t]s don't require any additional resources
    and allow for iteration to be started on demand, others such as [Rpc.Pipe_rpc.t]s or
    [Kafka.Reader.With_iterator.t]s may warrant not only some additional setup (such as
    establishing an [Rpc.Connection.t]) but also passing some [f] to an API in order to do
    so (such as dispatching via [Rpc.Pipe_rpc.dispatch_iter]). This is at odds with the
    standard [create_producer], which does not expose a way to distinguish between
    "resource acquisition failed" and "the iteration stopped".

    Instead, [create_producer_staged] allows this process to be broken up into multiple
    steps. The [f] passed to [iter] should not be called until [start] is determined.
    Internally, it is a lazy which will be replaced with the actual iteration function
    once the producer is started, which will be indicated by [start].

    The resulting [unit Or_error.t Deferred.t Or_error.t Deferred.t] should represent the
    two-stage nature of the producer:
    - [Error _] corresponds to resource acquisition failure, and will be returned by
      [create_producer_staged] itself
    - [Ok stopped] corresponds to resource acquisition success, where [stopped] is the
      ultimate outcome of the iteration. [create_producer_staged] will return
      [Ok producer], and the result of [start producer consumer] will be [stopped]. *)
val create_producer_staged
  :  iter:
       (start:unit Deferred.t
        -> f:('a f[@mode m])
        -> stop:unit Deferred.t
        -> unit Or_error.t Deferred.t Or_error.t Deferred.t)
  -> ('a Producer.t[@mode m]) Or_error.t Deferred.t

(** [create_producer_staged'] is like [create_producer_staged], but exposes more precise
    control over how the internal iteration is started and stopped.

    The ['state Or_error.t Deferred.t] produced by [iter ~f] should represent success or
    failure of resource acquisition. The [unit Or_error.t Deferred.t] produced by
    [start state ~stop] should represent the ultimate outcome of the iteration. *)
val create_producer_staged'
  :  iter:(f:('a f'[@mode m]) -> 'state Or_error.t Deferred.t)
  -> start:('state -> stop:unit Deferred.Choice.t list -> unit Or_error.t Deferred.t)
  -> ('a Producer.t[@mode m]) Or_error.t Deferred.t

(** It is guaranteed that [f] is not called after [stop] is determined. Iteration begins
    stopping once [stop] is determined. *)
val create_consumer
  :  f:('a f[@mode m])
  -> ?stop:_ Deferred.t
  -> unit
  -> ('a Consumer.t[@mode m])

(** [create_consumer'] is like [create_consumer], but does NOT check whether any [stop] is
    determined before calling [f]. Instead, users have direct control via [f], and can
    return [Stop]. The provided [stop] choices can be used to stop the iteration in the
    case when we are waiting for events to become available from the producer. When there
    are events available, [f] will be called regardless of [stop], and users are
    responsible for returning [Stop] as appropriate. *)
val create_consumer'
  :  f:('a f'[@mode m])
  -> ?stop:unit Deferred.Choice.t list
  -> unit
  -> ('a Consumer.t[@mode m])

(** Note that the [stop] conditions supplied to [create] and [create'] are not checked
    before calling the [f] supplied to ([contra_]) [inspect], [filter], [map],
    [filter_map], and [concat_map]. *)

type ('a, 'b, 'c) op :=
  ('a Producer.t[@mode m]) -> f:('a @ m -> 'b @ m) -> ('c Producer.t[@mode m])

val inspect : (('a, unit, 'a) op[@mode m])
val filter : (('a, bool, 'a) op[@mode m])
val map : (('a, 'b, 'b) op[@mode m])
val filter_map : (('a, 'b option, 'b) op[@mode m])
val concat_map : (('a, 'b list, 'b) op[@mode m])

type ('a, 'b, 'c) contra_op :=
  ('c Consumer.t[@mode m]) -> f:('a @ m -> 'b @ m) -> ('a Consumer.t[@mode m])

val contra_inspect : (('a, unit, 'a) contra_op[@mode m])
val contra_filter : (('a, bool, 'a) contra_op[@mode m])
val contra_map : (('a, 'b, 'b) contra_op[@mode m])
val contra_filter_map : (('a, 'b option, 'b) contra_op[@mode m])
val contra_concat_map : (('a, 'b list, 'b) contra_op[@mode m])]

val of_pipe_reader
  :  ?flushed:Pipe.Flushed.t (** default: [When_value_processed] *)
  -> 'message Pipe.Reader.t
  -> 'message Producer.t

val of_pipe_writer : 'message Pipe.Writer.t -> 'message Consumer.t

val of_async_writer
  :  ?flush_every:int (** default: flush after every write *)
  -> ?on_flush:(unit -> unit Maybe_pushback.t) (** called before waiting for flush *)
  -> Writer.t
  -> write:(Writer.t -> 'message -> unit)
  -> 'message Consumer.t

val of_direct_stream_writer
  :  ?flush_every:int (** default: flush after every write *)
  -> ?on_flush:(unit -> unit Maybe_pushback.t) (** called before waiting for flush *)
  -> 'message Rpc.Pipe_rpc.Direct_stream_writer.t
  -> 'message Consumer.t

val of_sequence : 'message Sequence.t -> 'message Producer.t

(** Convenience wrapper for iterating over batches (queues) of messages. *)
module Batched : sig
  module Producer : sig
    type 'a t = 'a Queue.t Producer.t
  end

  module Consumer : sig
    type 'a t = 'a Queue.t Consumer.t
  end

  val inspect : 'a Producer.t -> f:('a -> unit) -> 'a Producer.t
  val filter : 'a Producer.t -> f:('a -> bool) -> 'a Producer.t
  val map : 'a Producer.t -> f:('a -> 'b) -> 'b Producer.t
  val filter_map : 'a Producer.t -> f:('a -> 'b option) -> 'b Producer.t
  val concat_map : 'a Producer.t -> f:('a -> 'b list) -> 'b Producer.t
  val contra_inspect : 'a Consumer.t -> f:('a -> unit) -> 'a Consumer.t
  val contra_filter : 'a Consumer.t -> f:('a -> bool) -> 'a Consumer.t
  val contra_map : 'b Consumer.t -> f:('a -> 'b) -> 'a Consumer.t
  val contra_filter_map : 'b Consumer.t -> f:('a -> 'b option) -> 'a Consumer.t
  val contra_concat_map : 'b Consumer.t -> f:('a -> 'b list) -> 'a Consumer.t

  val of_pipe_reader
    :  ?flushed:Pipe.Flushed.t (** default: [When_value_processed] *)
    -> 'message Pipe.Reader.t
    -> 'message Producer.t

  val of_pipe_writer : 'message Pipe.Writer.t -> 'message Consumer.t

  val of_async_writer
    :  ?flush_every:int (** default: flush after every write *)
    -> ?on_flush:(unit -> unit Maybe_pushback.t) (** called before waiting for flush *)
    -> Writer.t
    -> write:(Writer.t -> 'message -> unit)
    -> 'message Consumer.t

  val of_direct_stream_writer
    :  ?flush_every:int (** default: flush after every write *)
    -> ?on_flush:(unit -> unit Maybe_pushback.t) (** called before waiting for flush *)
    -> 'message Rpc.Pipe_rpc.Direct_stream_writer.t
    -> 'message Consumer.t
end

(** [start producer consumer] takes the [f] and [stop] contained within [consumer] and
    passes them to the [iter] function contained within [producer], returning the eventual
    outcome of the iteration.

    See the documentation of [create_producer] re: calling [start] multiple times on the
    same producer. *)
val start : 'f Producer.t' -> 'f Consumer.t' -> unit Or_error.t Deferred.t

(** [create_producer_with_resource] is like the [with_] idiom or [With_resource] library,
    but delays cleanup until after the iteration has completed. This avoids problems a la
    dispatching a pipe RPC within [Rpc.Connection.with_client], which will close the
    connection when the dispatch has completed, rather than when one is done with the
    pipe. *)
val create_producer_with_resource
  :  (unit -> 'resource Or_error.t Deferred.t)
  -> close:('resource -> unit Or_error.t Deferred.t)
  -> create:('resource -> 'f Producer.t' Or_error.t Deferred.t)
  -> 'f Producer.t' Or_error.t Deferred.t

[%%template:
[@@@mode.default m = (global, local)]

(** Starts the producer with a null consumer that immediately stops. Useful for freeing up
    resources acquired by the producer. *)
val abort : (_ Producer.t[@mode m]) -> unit Or_error.t Deferred.t

(** [add_start] causes the producer to either wait for [start] to be determined before
    beginning iteration, or [abort] itself if the consumer is stopped, whichever happens
    first. *)
val add_start : ('a Producer.t[@mode m]) -> start:_ Deferred.t -> ('a Producer.t[@mode m])

(** The iterator will stop when any of its [stop] conditions are determined. [stop] will
    be checked before passing any message on to the original iterator. *)
val add_stop : ('a Consumer.t[@mode m]) -> stop:_ Deferred.t -> ('a Consumer.t[@mode m])

(** [add_stop'] is like [add_stop], but like [create_consumer'], does not check whether
    the [stop] conditions are determined before calling [f]. *)
val add_stop'
  :  ('a Consumer.t[@mode m])
  -> stop:unit Deferred.Choice.t list
  -> ('a Consumer.t[@mode m])]

val coerce_producer
  : [%template: ('a Producer.t[@mode global]) -> ('a Producer.t[@mode local])]

val coerce_consumer
  : [%template: ('a Consumer.t[@mode local]) -> ('a Consumer.t[@mode global])]

val unwrap_producer
  : [%template:
      ('a Modes.Global.t Producer.t[@mode local]) -> ('a Producer.t[@mode global])]

val wrap_consumer
  : [%template:
      ('a Consumer.t[@mode global]) -> ('a Modes.Global.t Consumer.t[@mode local])]

(** Some libraries may wish to share the same iterator between multiple independent
    iterations. For example, if one is iterating over several pipe readers in parallel,
    each pipe will respect the pushback it sees on its own iteration, but pushing back on
    one pipe will have no effect on others.

    In most scenarios this isn't much of a problem, as pushback is provided by e.g. a
    single pipe writer that serves as a sink for all iterations, naturally providing
    sequencing and a single source of pushback.

    Still, some clients may find it useful to sequence all calls to the iterator's
    internal [f], such that subsequent calls must always wait for previous calls to finish
    pushing back, regardless of whether they were invoked by a different iteration. *)

(** [pre_sequence consumer] causes the iterator to wait for the previous call to [f] to
    stop pushing back before calling [f] again. Thus only one call to [f] may be in flight
    at time, but subsequent messages may need to be buffered, so they cannot be locally
    allocated. *)
val pre_sequence : 'a Consumer.t -> 'a Consumer.t

(** [post_sequence consumer] causes the iterator to immediately call [f] on each new
    message, but then push back until both it and all previous calls have stopped pushing
    back. This allows it to still take locally-allocated messages, but more than one call
    to [f] may be in flight at a time. *)
val post_sequence
  : [%template: ('a Consumer.t[@mode local]) -> ('a Consumer.t[@mode local])]

module Helpers : sig
  (** [upon'] is provided as a utility for custom producer and consumer implementations.
      It's analogous to [Deferred.upon], but runs the provided function if any of the
      choices are determined. The advantage of [upon' choices f] over
      [upon (choose choices) f] is that the latter introduces an intermediate deferred and
      requires an additional async job before [f] is run. *)
  val upon' : unit Deferred.Choice.t list -> (unit -> unit) -> unit
end
