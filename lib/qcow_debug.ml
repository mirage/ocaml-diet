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

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

module Error = Qcow_error
module Physical = Qcow_physical
module Metadata = Qcow_metadata
open Qcow_types

let on_duplicate_reference metadata cluster_map (c, w) (c', w') target =
  let open Error.Lwt_write_error.Infix in
  let rec follow (c, w) (target: Physical.t) =
    Metadata.read metadata c
      (fun contents ->
        let p = Metadata.Physical.of_contents contents in
        let target' = Metadata.Physical.get p w in
        let descr = Printf.sprintf "Physical.get %s:%d = %s (%s %s)"
          (Cluster.to_string c) w (Physical.to_string target')
          (if target = target' then "=" else "<>")
          (Physical.to_string target) in
        if target <> target'
        then Log.err (fun f -> f "%s" descr)
        else Log.info (fun f -> f "%s" descr);
        Lwt.return (Ok ())
      )
    >>= fun () ->
    match Qcow_cluster_map.find cluster_map c with
    | exception Not_found ->
      Log.err (fun f -> f "No reference to cluster %s" (Cluster.to_string c));
      Lwt.return (Ok ())
    | c', w' -> follow (c', w') (Physical.make ~is_mutable:true ~is_compressed:false @@ Cluster.to_int c) in
  let target = Physical.make ~is_mutable:true ~is_compressed:true @@ Int64.to_int target in
  follow (Cluster.of_int64 c', w') target
  >>= fun () ->
  follow (Cluster.of_int64 c, w) target
