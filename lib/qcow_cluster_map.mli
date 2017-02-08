(*
 * Copyright (C) 2016 David Scott <dave@recoil.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *)
open Qcow_types

type t
(** A cluster map which describes cluster usage in the file. The cluster map
    tracks which clusters are free, and which are used, and where the references
    are. *)

type move_state =
  | Copying
  (** a background copy is in progress. If this cluster is modified then
      the copy should be aborted. *)
  | Copied
  (** contents of this cluster have been copied once to another cluster.
      If this cluster is modified then the copy should be aborted. *)
  | Flushed
  (** contents of this cluster have been copied and flushed to disk: it
      is now safe to rewrite the pointer. If this cluster is modified then
      the copy should be aborted. *)
  | Referenced
  (** the reference has been rewritten; it is now safe to write to this
      cluster again. On the next flush, the copy is complete and the original
      block can be recycled. *)
(** Describes the state of a block move *)

type cluster = int64

type reference = cluster * int (* cluster * index within cluster *)

module ClusterMap: Map.S with type key = cluster

module Move: sig
  type t = { src: cluster; dst: cluster }
  (** An instruction to move the contents from cluster [src] to cluster [dst] *)
end

type move = {
  move: Move.t;
  state: move_state;
}
(** describes the state of an in-progress block move *)

val zero: t
(** A cluster map for a zero-length disk *)

val make: free:Qcow_bitmap.t -> refs:reference ClusterMap.t -> first_movable_cluster:cluster -> t
(** Given a set of free clusters, and the first cluster which can be moved
    (i.e. that isn't fixed header), construct an empty cluster map. *)

val total_used: t -> int64
(** Return the number of tracked used clusters *)

val total_free: t -> int64
(** Return the number of tracked free clusters *)

val resize: t -> cluster -> unit
(** [resize t new_size_clusters] is called when the file is to be resized. *)

val add: t -> reference -> cluster -> unit
(** [add t ref cluster] marks [cluster] as in-use and notes the reference from
    [reference]. *)

val remove: t -> cluster -> unit
(** [remove t cluster] marks [cluster] as free and invalidates any reference
    to it (e.g. in response to a discard) *)

val junk: t -> Int64.IntervalSet.t
(** [junk t] returns the set of clusters containing junk data *)

val add_to_junk: t -> Int64.IntervalSet.t -> unit
(** [add_to_junk t more] adds [more] to the clusters known to contain junk data
    which must be overwritten before they can be reused. *)

val remove_from_junk: t -> Int64.IntervalSet.t -> unit
(** [remove_from_junk t less] removes [less] from the clusters known to contain
    junk data which must be overwritten before they can be reused. *)

val wait: t -> unit Lwt.t
(** [wait t] wait for the number of junk, erased or available sets to change. *)

val available: t -> Int64.IntervalSet.t
(** [available t] returns the set of clusters which are available for reallocation *)

val add_to_available: t -> Int64.IntervalSet.t -> unit
(** [add_to_available t more] adds [more] to the clusters known to contain
    available data *)

val remove_from_available: t -> Int64.IntervalSet.t -> unit
(** [remove_from_available t less] removes [less] from the clusters known to
    contain available data *)

val erased: t -> Int64.IntervalSet.t
(** [erased t] returns the set of clusters which are erased but not yet flushed *)

val add_to_erased: t -> Int64.IntervalSet.t -> unit
(** [add_to_erased t more] adds [more] to the clusters which have been erased *)

val remove_from_erased: t -> Int64.IntervalSet.t -> unit
(** [remove_from_erased t less] removes [less] from the clusters which have been
    erased *)

val moves: t -> move Int64.Map.t

val set_move_state: t -> Move.t -> move_state -> unit
(** Update the state of the given move operation *)

val cancel_move: t -> int64 -> unit
(** [cancel_move cluster] cancels any in-progress move of cluster [cluster].
    This should be called with the cluster write lock held whenever there has
    been a change in the contents of [cluster] *)

val complete_move: t -> Move.t -> unit
(** [complete_move t move] marks the move as complete. *)

val find: t -> cluster -> reference
(** [find t cluster] returns the reference to [cluster], or raises [Not_found] *)

val with_roots: t -> Int64.IntervalSet.t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
(** [with_roots t clusters f] calls [f ()} with [clusters] registered as in-use. *)

val compact_s: (Move.t -> 'a -> ((bool * 'a), 'b) result Lwt.t ) -> t -> 'a
  -> ('a, 'b) result Lwt.t
(** [compact_s f t acc] accumulates the result of [f move] where [move] is
    the next cluster move needed to perform a compaction of [t].. *)

val get_last_block: t -> int64
(** [get_last_block t] is the last allocated block in [t]. Note if there are no
    data blocks this will point to the last header block even though it is
    immovable. *)

val to_summary_string: t -> string
(** [to_summary_string t] returns a terse printable summary of [t] *)
