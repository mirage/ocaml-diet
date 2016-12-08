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

type t
(** A cluster map which describes cluster usage in the file. The cluster map
    tracks which clusters are free, and which are used, and where the references
    are. *)

type cluster = int64

type reference = cluster * int (* cluster * offset within cluster *)

module ClusterSet: Qcow_s.INTERVAL_SET with type elt = cluster

module ClusterMap: Map.S with type key = cluster

val make: free:ClusterSet.t -> first_movable_cluster:cluster -> t
(** Given a set of free clusters, and the first cluster which can be moved
    (i.e. that isn't fixed header), construct an empty cluster map. *)

val get_free: t -> ClusterSet.t
(** Return the current set of free clusters *)

val get_references: t -> reference ClusterMap.t
(** Return the current map of references *)

val get_first_movable_cluster: t -> cluster
(** Return the first movable cluster *)

val add: t -> reference -> cluster -> t
(** [add t ref cluster] marks [cluster] as in-use and notes the reference from
    [reference]. *)

val fold_over_free: (cluster -> 'a -> 'a) -> t -> 'a -> 'a
(** [fold_over_free f t acc] folds [f] over all the free clusters in [t] *)
