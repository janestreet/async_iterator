include Async_iterator
include Composition_infix
open! Core
open! Async

let print_stop stop = print_s [%sexp "stop", (stop : unit Deferred.t)]

let print_pushback pushback =
  print_s [%sexp "pushback", (pushback : unit Maybe_pushback.t)]
;;

let print_action action =
  print_s [%sexp "action", (Iterator.Action.globalize action : Iterator.Action.t)]
;;

let print_stopped stopped =
  print_s [%sexp "stopped", (stopped : unit Or_error.t Deferred.t)]
;;

let print_closed closed = print_s [%sexp "closed", (closed : unit Deferred.t)]

let print_flushed flushed =
  print_s [%sexp "flushed", (flushed : Pipe.Flushed_result.t Deferred.t)]
;;
