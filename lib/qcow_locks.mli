(*
 * Copyright (C) 2017 David Scott <dave@recoil.org>
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
(** A set of per-cluster read and write locks *)

val make: unit -> t
(** Create a set of locks *)

val with_read_lock: t -> Cluster.t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
(** [with_read_lock t f] executes [f ()] with the lock held for reading *)

val with_read_locks: t -> first:Cluster.t -> last:Cluster.t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
(** [with_read_locks t ~first ~last f] executes [f ()] with all clusters in the
    interval [first .. last] inclusive locked for reading. *)

val with_write_lock: t -> Cluster.t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
(** [with_write_lock t f] executes [f ()] with the lock held for writing *)

val with_write_locks: t -> first:Cluster.t -> last:Cluster.t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
(** [with_write_locks t ~first ~last f] executes [f ()] with all clusters in the
    interval [first .. last] inclusive locked for writing. *)

val with_metadata_lock: t -> (unit -> 'a Lwt.t) -> 'a Lwt.t
(** [with_metadata_lock t f] executes [f ()] with the global metadata lock held.
    This prevents metadata blocks from moving while they're being used. *)
