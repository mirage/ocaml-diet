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

module Make(Time: Mirage_time_lwt.S): sig

  type t
  (** A timer which runs a background task after a set duration. The timer can
      be reset at any time. The timer guarantees to wait at least the set
      duration before running the task again. *)

  val make: f:(unit -> unit Lwt.t) -> description:string -> unit -> t
  (** Create timer which is initially stopped. *)

  val restart: t -> duration_ms:int -> unit
  (** Restart the timer.

      If the task is running now then another will be scheduled
      to start [duration_ms] milliseconds after the running one has finished.

      If no task is running then any existing scheduled task is removed and a
      new one is scheduled for [duration_ms] from now.
  *)

  val cancel: t -> unit
  (** If an operation is in progress, cancel and reschedule it. *)
end
