open! Core
open! Async
open! Import

module type S = sig
  type action
  type +'message producer
  type -'message consumer
  type 'message wrapper
  type ('a, 'b) create_f
  type ('a, 'b) create'_f
  type ('a, 'b) op_f

  (** A [Producer.t] represents the input of an iteration. For example, one might want to
      read from a pipe, or from a database, or from a value generator. Generalizing over
      these sources allows us to "connect" them to consumers.

      [Producer.t] and the functions which operate on it are analogous to [Pipe.Reader.t]
      and related functions in the [Pipe] module. *)
  module Producer : sig
    type 'message t = 'message wrapper producer
  end

  (** A [Consumer.t] represents the output of an iteration. For example, one might want to
      write to a pipe, or to a database, or do some cheap synchronous operation.
      Generalizing over these sinks allows us to "connect" them to producers.

      [Consumer.t] and the functions which operate on it are analogous to [Pipe.Writer.t]
      and related functions in the [Pipe] module. *)
  module Consumer : sig
    type 'message t = 'message wrapper consumer
  end

  (** [create_producer] describes how to iterate over some data source, passing each
      message to a supplied consumer. The resulting [unit Or_error.t Deferred.t] should
      be determined when the iteration has stopped.

      [start (create_producer ~iter) (create_consumer ~f ~stop ())] effectively results
      in the same behavior as calling [iter ~f ~stop] directly.

      Like [Pipe.iter], producers are not designed to be [start]ed more than once. The
      behavior in this scenario is left unspecified. However, a good rule of thumb is that
      [stop]ping should cause [iter] to clean up any resources (e.g. calling
      [Pipe.close_read]), and several implementations in this module simply raise if the
      same producer is started multiple times. *)
  val create_producer
    :  iter:
         (f:('message, unit Maybe_pushback.t) create_f
          -> stop:unit Deferred.t
          -> unit Or_error.t Deferred.t)
    -> 'message Producer.t

  (** [create_producer'] is like [create_producer], but exposes more precise control over
      stopping behavior. *)
  val create_producer'
    :  iter:
         (f:('message, action) create'_f
          -> stop:unit Deferred.Choice.t list
          -> unit Or_error.t Deferred.t)
    -> 'message Producer.t

  (** While some producers such as [Pipe.Reader.t]s don't require any additional resources
      and allow for iteration to be started on demand, others such as [Rpc.Pipe_rpc.t]s or
      [Kafka.Reader.With_iterator.t]s may warrant not only some additional setup (such as
      establishing an [Rpc.Connection.t]) but also passing some [f] to an API in order to
      do so (such as dispatching via [Rpc.Pipe_rpc.dispatch_iter]). This is at odds with
      the standard [create_producer], which does not expose a way to distinguish between
      "resource acquisition failed" and "the iteration stopped".

      Instead, [create_producer_staged] allows this process to be broken up into multiple
      steps. The [f] passed to [iter] should not be called until [start] is determined.
      Internally, it is a lazy which will be replaced with the actual iteration function
      once the producer is started, which will be indicated by [start].

      The resulting [unit Or_error.t Deferred.t Or_error.t Deferred.t] should represent
      the two-stage nature of the producer:
      - [Error _] corresponds to resource acquisition failure, and will be returned by
        [create_producer_staged] itself
      - [Ok stopped] corresponds to resource acquisition success, where [stopped] is the
        ultimate outcome of the iteration. [create_producer_staged] will return
        [Ok producer], and the result of [start producer consumer] will be [stopped]. *)
  val create_producer_staged
    :  iter:
         (start:unit Deferred.t
          -> f:('message, unit Maybe_pushback.t) create_f
          -> stop:unit Deferred.t
          -> unit Or_error.t Deferred.t Or_error.t Deferred.t)
    -> 'message Producer.t Or_error.t Deferred.t

  (** [create_producer_staged'] is like [create_producer_staged], but exposes more precise
      control over how the internal iteration is started and stopped.

      The ['state Or_error.t Deferred.t] produced by [iter ~f] should represent success or
      failure of resource acquisition. The [unit Or_error.t Deferred.t] produced by
      [start state ~stop] should represent the ultimate outcome of the iteration. *)
  val create_producer_staged'
    :  iter:(f:('message, action) create'_f -> 'state Or_error.t Deferred.t)
    -> start:('state -> stop:unit Deferred.Choice.t list -> unit Or_error.t Deferred.t)
    -> 'message Producer.t Or_error.t Deferred.t

  (** It is guaranteed that [f] is not called after [stop] is determined. Iteration begins
      stopping once [stop] is determined. *)
  val create_consumer
    :  f:('message, unit Maybe_pushback.t) create_f
    -> ?stop:_ Deferred.t (** default: never *)
    -> unit
    -> 'message Consumer.t

  (** [create_consumer'] is like [create_consumer], but does NOT check whether any [stop]
      is determined before calling [f]. Instead, users have direct control via [f], and
      can return [Stop]. The provided [stop] choices can be used to stop the iteration in
      the case when we are waiting for events to become available from the producer. When
      there are events available, [f] will be called regardless of [stop], and users are
      responsible for returning [Stop] as appropriate. *)
  val create_consumer'
    :  f:('message, action) create'_f
    -> ?stop:unit Deferred.Choice.t list (** default: never *)
    -> unit
    -> 'message Consumer.t

  (** Note that the [stop] conditions supplied to [create] and [create'] are not checked
      before calling the [f] supplied to ([contra_]) [inspect], [filter], [map],
      [filter_map], and [concat_map]. *)

  val inspect : 'message Producer.t -> f:('message, unit) op_f -> 'message Producer.t
  val filter : 'message Producer.t -> f:('message, bool) op_f -> 'message Producer.t
  val map : 'a Producer.t -> f:('a, 'b) op_f -> 'b Producer.t
  val filter_map : 'a Producer.t -> f:('a, 'b option) op_f -> 'b Producer.t
  val concat_map : 'a Producer.t -> f:('a, 'b list) op_f -> 'b Producer.t

  val contra_inspect
    :  'message Consumer.t
    -> f:('message, unit) op_f
    -> 'message Consumer.t

  val contra_filter
    :  'message Consumer.t
    -> f:('message, bool) op_f
    -> 'message Consumer.t

  val contra_map : 'b Consumer.t -> f:('a, 'b) op_f -> 'a Consumer.t
  val contra_filter_map : 'b Consumer.t -> f:('a, 'b option) op_f -> 'a Consumer.t
  val contra_concat_map : 'b Consumer.t -> f:('a, 'b list) op_f -> 'a Consumer.t
end

module type S_global = sig
  include S (** @inline *)

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

module type Iterator = sig
  module Action : sig
    type t =
      | Stop
      | Continue
      | Wait of { global_ pushback : unit Deferred.t }
    [@@deriving globalize, sexp_of]
  end

  module Producer : sig
    type +'message t
  end

  module Consumer : sig
    type -'message t
  end

  (** @inline *)
  module type S =
    S
    with type action := Action.t
     and type 'message producer := 'message Producer.t
     and type 'message consumer := 'message Consumer.t

  (** @inline *)
  module type S_global =
    S_global
    with type action := Action.t
     and type 'message producer := 'message Producer.t
     and type 'message consumer := 'message Consumer.t

  (** [start producer consumer] takes the [f] and [stop] contained within [consumer] and
      passes them to the [iter] function contained within [producer], returning the
      eventual outcome of the iteration.

      See the documentation of [create_producer] re: calling [start] multiple times on the
      same producer. *)
  val start : 'message Producer.t -> 'message Consumer.t -> unit Or_error.t Deferred.t

  (** @inline *)
  module Local :
    S
    with type 'message wrapper := 'message
     and type ('a, 'b) create_f := local_ 'a -> 'b
     and type ('a, 'b) create'_f := local_ 'a -> local_ 'b
     and type ('a, 'b) op_f := local_ 'a -> local_ 'b

  module Global : sig
    (** @inline *)
    include
      S_global
      with type 'message wrapper := 'message Modes.Global.t
       and type ('a, 'b) create_f := 'a -> 'b
       and type ('a, 'b) create'_f := 'a -> local_ 'b
       and type ('a, 'b) op_f := 'a -> 'b

    val of_sequence : 'message Sequence.t -> 'message Producer.t
  end

  (** @inline *)
  module Batched :
    S_global
    with type 'message wrapper := 'message Queue.t Modes.Global.t
     and type ('a, 'b) create_f := 'a Queue.t -> 'b
     and type ('a, 'b) create'_f := 'a Queue.t -> local_ 'b
     and type ('a, 'b) op_f := 'a -> 'b

  (** [create_producer_with_resource] is like the [with_] idiom or
      [With_resource] library, but delays cleanup until after the iteration has completed.
      This avoids problems a la dispatching a pipe RPC within
      [Rpc.Connection.with_client], which will close the connection when the dispatch has
      completed, rather than when one is done with the pipe. *)
  val create_producer_with_resource
    :  (unit -> 'resource Or_error.t Deferred.t)
    -> close:('resource -> unit Or_error.t Deferred.t)
    -> create:('resource -> 'message Producer.t Or_error.t Deferred.t)
    -> 'message Producer.t Or_error.t Deferred.t

  (** Starts the producer with a null consumer that immediately stops. Useful for freeing
      up resources acquired by the producer. *)
  val abort : _ Producer.t -> unit Or_error.t Deferred.t

  (** [add_start] causes the producer to either wait for [start] to be determined before
      beginning iteration, or [abort] itself if the consumer is stopped, whichever happens
      first. *)
  val add_start : 'message Producer.t -> start:_ Deferred.t -> 'message Producer.t

  (** The iterator will stop when any of its [stop] conditions are determined. [stop] will
      be checked before passing any message on to the original iterator. *)
  val add_stop : 'message Consumer.t -> stop:_ Deferred.t -> 'message Consumer.t

  (** [add_stop'] is like [add_stop], but like [create_consumer'], does not check whether
      the [stop] conditions are determined before calling [f]. *)
  val add_stop'
    :  'message Consumer.t
    -> stop:unit Deferred.Choice.t list
    -> 'message Consumer.t

  val coerce_producer : 'message Global.Producer.t -> 'message Local.Producer.t
  val coerce_consumer : 'message Local.Consumer.t -> 'message Global.Consumer.t

  (** Some libraries may wish to share the same iterator between multiple independent
      iterations. For example, if one is iterating over several pipe readers in parallel,
      each pipe will respect the pushback it sees on its own iteration, but pushing back
      on one pipe will have no effect on others.

      In most scenarios this isn't much of a problem, as pushback is provided by e.g. a
      single pipe writer that serves as a sink for all iterations, naturally providing
      sequencing and a single source of pushback.

      Still, some clients may find it useful to sequence all calls to the iterator's
      internal [f], such that subsequent calls must always wait for previous calls to
      finish pushing back, regardless of whether they were invoked by a different
      iteration. *)

  (** [pre_sequence consumer] causes the iterator to wait for the previous call to [f] to
      stop pushing back before calling [f] again. Thus only one call to [f] may be in
      flight at time, but subsequent messages may need to be buffered, so they cannot be
      locally allocated. *)
  val pre_sequence : 'message Global.Consumer.t -> 'message Global.Consumer.t

  (** [post_sequence consumer] causes the iterator to immediately call [f] on each new
      message, but then push back until both it and all previous calls have stopped
      pushing back. This allows it to still take locally-allocated messages, but more than
      one call to [f] may be in flight at a time. *)
  val post_sequence : 'message Consumer.t -> 'message Consumer.t

  module Helpers : sig
    (** [upon'] is provided as a utility for custom producer and consumer implementations.
        It's analogous to [Deferred.upon], but runs the provided function if any of the
        choices are determined. The advantage of [upon' choices f] over [upon (choose
        choices) f] is that the latter introduces an intermediate deferred and requires an
        additional async job before [f] is run. *)
    val upon' : unit Deferred.Choice.t list -> (unit -> unit) -> unit
  end
end
