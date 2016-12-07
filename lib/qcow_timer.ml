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

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

module Make(Time: V1_LWT.TIME) = struct

  type t = {
    description: string;
    mutable timer: unit Lwt.t;
    f: unit -> unit Lwt.t;
    mutable task_running: bool;
    mutable please_schedule_another: bool;
  }

  let make ~f ~description () =
    let timer = Lwt.return_unit in
    let task_running = false in
    let please_schedule_another = false in
    { description; timer; f; task_running; please_schedule_another }

  let restart t ~duration_ms =
    let open Lwt.Infix in
    Lwt.cancel t.timer;
    match t.task_running with
    | true ->
      t.please_schedule_another <- true
    | false ->
      let timer = Time.sleep (float_of_int duration_ms /. 1000.0) in
      t.timer <- timer;
      let rec loop () =
        Lwt.catch
          (fun () ->
            timer
            >>= fun () ->
            t.task_running <- true;
            Log.info (fun f -> f "running background %s" t.description);
            Lwt.catch
              (fun () ->
                t.f ()
                >>= fun () ->
                Log.info (fun f -> f "background %s successful" t.description);
                Lwt.return_unit
              )
              (fun e ->
                Log.err (fun f -> f "background %s failed with: %s" t.description (Printexc.to_string e));
                Lwt.return_unit
              )
            >>= fun () ->
            Lwt.return_unit
          ) (function
            | Lwt.Canceled -> Lwt.return_unit
            | e ->
              Log.err (fun f -> f "background %s timer failed with: %s" t.description (Printexc.to_string e));
              Lwt.return_unit
          )
        >>= fun () ->
        t.task_running <- false;
        if t.please_schedule_another then begin
          t.please_schedule_another <- false;
          loop ()
        end else
          Lwt.return_unit in
      Lwt.async loop
end
