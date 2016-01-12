(*
 * Copyright (C) 2016 Unikernel Systems
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 *)

let debug fmt =
  Printf.ksprintf (fun s ->
      Printf.fprintf stderr "%s\n%!" s
    ) fmt

let check_exit_status cmd (out, err) status =
  if out <> [] then debug "stdout: %s" (String.concat "\n" out);
  if err <> [] then debug "stderr: %s" (String.concat "\n" err);
  match status with
  | Unix.WEXITED 0   -> `Ok (out, err)
  | _ -> debug "%s failed" cmd; `Error (`Msg cmd)

let read_lines oc =
  let rec aux acc =
    let line =
      try Some (input_line oc)
      with End_of_file -> None
    in
    match line with
    | Some l -> aux (l :: acc)
    | None   -> List.rev acc
  in
  aux []

let or_failwith = function
  | `Ok x -> x
  | `Error (`Msg m) -> failwith m

let ignore_output (_: (string list * string list)) = ()

let run cmd args =
  let args' = List.map (fun x -> "'" ^ (String.escaped x) ^ "'") args in
  let cmdline = Printf.sprintf "%s %s " cmd (String.concat " " args') in
  debug "%s" cmdline;
  let oc, ic, ec = Unix.open_process_full cmdline (Unix.environment ()) in
  let out = read_lines oc in
  let err = read_lines ec in
  let exit_status = Unix.close_process_full (oc, ic, ec) in
  ignore_output @@ or_failwith @@ check_exit_status cmdline (out, err) exit_status
