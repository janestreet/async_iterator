open! Core
open! Async

(** Configures whether workers support state updates. [Not_using] means workers don't
    support state updates. [Using] means they do, and requires an update function and
    serializer. When [Not_using], the [state_update] type becomes [Nothing.t], making it
    impossible to call [update_worker_state]. *)
type ('state, 'state_update) t =
  | Not_using : (_, Nothing.t) t
  | Using :
      { update_worker_state : 'state -> 'state_update -> unit Or_error.t Deferred.t
      ; bin_state_update : 'state_update Bin_prot.Type_class.t
      }
      -> ('state, 'state_update) t

val resolve
  :  ('state, 'state_update) t
  -> ('state -> 'state_update -> unit Or_error.t Deferred.t)
     * 'state_update Bin_prot.Type_class.t
