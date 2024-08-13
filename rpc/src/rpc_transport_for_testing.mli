open! Core
open! Async
open! Import

(** Using a TCP-backed transport in tests is annoying, since it's hard to control the
    behavior of data buffered in the kernel. On the other hand,
    [Async_rpc_kernel.Pipe_transport] is not ideal for testing this library, since the
    writer has no pushback mechanism and the reader inserts a deferred bind after every
    message.

    Instead, this module provides a way to create in-process transports where
    [Writer.flushed] waits until all messages are read out of the transport, and
    [Reader.read_forever] synchronously reads the next message (if available) when
    [on_message] produces [Continue].

    Note that reads are still batched, i.e. the reader synchronously consumes all messages
    buffered in the transport, but then may perform an arbitrary number of deferred
    operations while handling each message.

    Conceptually, the behavior of these transports can be thought of as an
    [Async.Writer.t] that flushes its buffer directly into that of an [Async.Reader.t],
    without going through any intermediate file descriptor. This makes it pretty similar
    to a regular TCP-backed transport with the number of bytes in the kernel or on the
    wire guaranteed to be zero. However, if [on_message] produces [Wait _], we immediately
    bind on the deferred, whereas the standard TCP reader has no asynchrony while
    processing a batch of messages, and instead binds on them all at the end of the batch.
    The goal of this behavioral change is to make some of the test output clearer. *)
val client
  :  server_implementations:'server_connection_state Rpc.Implementations.t
  -> server_connection_state:(Rpc.Connection.t -> 'server_connection_state)
  -> Rpc.Connection.t Or_error.t Deferred.t
