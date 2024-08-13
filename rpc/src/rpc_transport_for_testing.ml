open! Core
open! Async
open! Import

module One_way : sig
  type -'perms t [@@deriving sexp_of]

  val create : unit -> read_write t
  val close : write t -> unit Deferred.t
  val close_read : read t -> unit Deferred.t
  val closed : _ t -> unit Deferred.t
  val is_closed : _ t -> bool
  val flushed : write t -> unit Deferred.t
  val write_exn : write t -> 'a Bin_prot.Writer.t -> 'a -> unit
  val values_available_or_read_closed : read t -> unit Deferred.t
  val read_now : read t -> Bigbuffer.t -> [ `Ok | `Nothing_available | `Eof ]
  val length_in_bytes : _ t -> int
end = struct
  type 'perms t =
    { buffer : Bigbuffer.t
    ; closed : unit Ivar.t
    ; flushed : (unit, read_write) Bvar.t
    ; values_available_or_read_closed : (unit, read_write) Bvar.t
    }
  [@@deriving sexp_of]

  let create () =
    { buffer = Bigbuffer.create 1024
    ; closed = Ivar.create ()
    ; flushed = Bvar.create ()
    ; values_available_or_read_closed = Bvar.create ()
    }
  ;;

  let clear t =
    Bigbuffer.clear t.buffer;
    Bvar.broadcast t.flushed ()
  ;;

  let flushed t = if Bigbuffer.length t.buffer = 0 then return () else Bvar.wait t.flushed

  let close t =
    Ivar.fill_if_empty t.closed ();
    flushed t
  ;;

  let close_read t =
    clear t;
    Bvar.broadcast t.values_available_or_read_closed ();
    close t
  ;;

  let closed t = Ivar.read t.closed
  let is_closed t = Ivar.is_full t.closed
  let values_available t = Bigbuffer.length t.buffer > 0

  let write_exn t writer x =
    assert (not (is_closed t));
    Bigbuffer.add_bin_prot t.buffer writer x;
    if values_available t then Bvar.broadcast t.values_available_or_read_closed ()
  ;;

  let values_available_or_read_closed t =
    if values_available t || is_closed t
    then return ()
    else Bvar.wait t.values_available_or_read_closed
  ;;

  let read_now t buffer =
    if values_available t
    then (
      Bigbuffer.add_buffer buffer t.buffer;
      clear t;
      `Ok)
    else if is_closed t
    then `Eof
    else `Nothing_available
  ;;

  let length_in_bytes t = Bigbuffer.length t.buffer
end

module Writer : sig
  include Rpc.Transport.Writer.S

  val create : [> write ] One_way.t -> t
end = struct
  type t =
    { writer : write One_way.t
    ; monitor : Monitor.t
    ; mutable bytes_written : int
    }
  [@@deriving sexp_of]

  let create writer =
    { writer :> write One_way.t; monitor = Monitor.create (); bytes_written = 0 }
  ;;

  let close t = One_way.close t.writer
  let is_closed t = One_way.is_closed t.writer
  let monitor t = t.monitor
  let bytes_to_write t = One_way.length_in_bytes t.writer
  let bytes_written t = Int63.of_int t.bytes_written
  let stopped t = One_way.closed t.writer
  let flushed t = One_way.flushed t.writer
  let ready_to_write t = flushed t

  let send_bin_prot t (writer : _ Bin_prot.Writer.t) x =
    if One_way.is_closed t.writer
    then Rpc.Transport.Send_result.Closed
    else (
      let len = writer.size x in
      t.bytes_written <- t.bytes_written + len;
      One_way.write_exn t.writer Bin_prot.Type_class.bin_writer_int_64bit len;
      One_way.write_exn t.writer writer x;
      Sent { result = (); bytes = len })
  ;;

  let bin_writer_bigsubstring_no_length : _ Bin_prot.Writer.t =
    { size = Bigsubstring.length
    ; write =
        (fun dst ~pos:dst_pos src ->
          Bigsubstring.blit_to_bigstring src ~dst ~dst_pos;
          dst_pos + Bigsubstring.length src)
    }
  ;;

  let send_bin_prot_and_bigstring t writer x ~buf ~pos ~len =
    send_bin_prot
      t
      (Bin_prot.Type_class.bin_writer_pair writer bin_writer_bigsubstring_no_length)
      (x, Bigsubstring.create buf ~pos ~len)
  ;;

  (* This function does actually copy the bigstring, so we wait for [flushed] to pretend
     like it doesn't. *)
  let send_bin_prot_and_bigstring_non_copying t writer x ~buf ~pos ~len =
    match send_bin_prot_and_bigstring t writer x ~buf ~pos ~len with
    | (Closed | Message_too_big _) as result -> result
    | Sent { result = (); bytes } -> Sent { result = flushed t; bytes }
  ;;
end

module Reader : sig
  include Rpc.Transport.Reader.S

  val create : [> read ] One_way.t -> t
end = struct
  type t =
    { reader : read One_way.t
    ; buffer : Bigbuffer.t
    ; pos_ref : int ref
    ; mutable bytes_read : int
    }
  [@@deriving sexp_of]

  let create reader =
    { reader :> read One_way.t
    ; buffer = Bigbuffer.create 1024
    ; pos_ref = ref 0
    ; bytes_read = 0
    }
  ;;

  let close t = One_way.close_read t.reader
  let is_closed t = One_way.is_closed t.reader
  let bytes_read t = Int63.of_int t.bytes_read

  let rec read_unbuffered t ~on_message ~on_end_of_batch =
    match One_way.read_now t.reader t.buffer with
    | `Ok -> read_buffered t ~on_message ~on_end_of_batch
    | `Nothing_available ->
      let%bind () = One_way.values_available_or_read_closed t.reader in
      read_unbuffered t ~on_message ~on_end_of_batch
    | `Eof -> return (Error `Eof)

  and read_buffered t ~on_message ~on_end_of_batch =
    if !(t.pos_ref) < Bigbuffer.length t.buffer
    then (
      let buffer = Bigbuffer.volatile_contents t.buffer in
      let len = Bin_prot.Utils.bin_read_size_header buffer ~pos_ref:t.pos_ref in
      t.bytes_read <- t.bytes_read + len;
      let pos = !(t.pos_ref) in
      t.pos_ref := pos + len;
      match on_message buffer ~pos ~len with
      | Rpc.Transport.Handler_result.Stop x -> return (Ok x)
      | Continue -> read_buffered t ~on_message ~on_end_of_batch
      | Wait wait ->
        let%bind () = wait in
        read_buffered t ~on_message ~on_end_of_batch)
    else (
      Bigbuffer.clear t.buffer;
      t.pos_ref := 0;
      on_end_of_batch ();
      read_unbuffered t ~on_message ~on_end_of_batch)
  ;;

  let read_forever t ~on_message ~on_end_of_batch =
    if !(t.pos_ref) < Bigbuffer.length t.buffer
    then read_buffered t ~on_message ~on_end_of_batch
    else read_unbuffered t ~on_message ~on_end_of_batch
  ;;
end

let client ~server_implementations ~server_connection_state =
  let transport r w : Rpc.Transport.t =
    { reader = Rpc.Transport.Reader.pack (module Reader) (Reader.create r)
    ; writer = Rpc.Transport.Writer.pack (module Writer) (Writer.create w)
    }
  in
  let a = One_way.create () in
  let b = One_way.create () in
  let%bind server_connection =
    Async_rpc_kernel.Rpc.Connection.create
      ~implementations:server_implementations
      ~connection_state:server_connection_state
      (transport a b)
    >>| Result.map_error ~f:Error.of_exn
  and client_connection =
    Async_rpc_kernel.Rpc.Connection.create
      ~connection_state:(fun (_ : Rpc.Connection.t) -> ())
      (transport b a)
    >>| Result.map_error ~f:Error.of_exn
  in
  match Or_error.both server_connection client_connection with
  | Error _ as error -> return error
  | Ok ((_ : Rpc.Connection.t), client_connection) -> return (Ok client_connection)
;;
