(*
 * Copyright (C) 2015 David Scott <dave@recoil.org>
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
open Sexplib.Std
open Result
open Qcow
open Error

let expect_ok = function
  | Ok x -> x
  | Error (`Msg m) -> failwith m

let (>>*=) m f =
  let open Lwt in
  m >>= function
  | `Error x -> Lwt.return (`Error x)
  | `Ok x -> f x

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

let to_cmdliner_error = function
  | `Error `Disconnected -> `Error(false, "Disconnected")
  | `Error `Is_read_only -> `Error(false, "Is_read_only")
  | `Error `Unimplemented -> `Error(false, "Unimplemented")
  | `Error (`Unknown x) -> `Error(false, x)
  | `Ok x -> `Ok x

module Block = struct
  include Block
  (* We're not interested in any optional arguments [connect] may or may not
     have *)
  let connect path = connect path
end

module TracedBlock = struct
  include Block

  let length_of bufs = List.fold_left (+) 0 (List.map Cstruct.len bufs)

  let read t sector bufs =
    Log.info (fun f -> f "BLOCK.read %Ld len = %d" sector (length_of bufs));
    read t sector bufs

  let write t sector bufs =
    Log.info (fun f -> f "BLOCK.write %Ld len = %d" sector (length_of bufs));
    write t sector bufs

  let flush t =
    Log.info (fun f -> f "BLOCK.flush");
    flush t

  let resize t new_size =
    Log.info (fun f -> f "BLOCK.resize %Ld" new_size);
    resize t new_size

end

module type BLOCK = sig

  include Qcow_s.RESIZABLE_BLOCK

  val connect: string -> [ `Ok of t | `Error of error ] Lwt.t
end

module UnsafeBlock = struct
  include Block
  let flush _ = Lwt.return (`Ok ())
end

let handle_common common_options_t =
  if common_options_t.Common.debug then begin
    List.iter
      (fun src ->
        if Logs.Src.name src = "qcow"
        then Logs.Src.set_level src (Some Logs.Debug)
      ) (Logs.Src.list ())
  end

let spinner = [| '-'; '\\'; '|'; '/' |]
let spinner_idx = ref 0
let progress_bar_width = 70
let progress_cb ~percent =
  let line = Bytes.make (progress_bar_width + 8) '\000' in

  let len = (progress_bar_width * percent) / 100 in
  for i = 0 to len - 1 do
    line.[4 + i] <- (if i = len - 1 then '>' else '#')
  done;
  line.[0] <- '[';
  line.[1] <- spinner.(!spinner_idx);
  line.[2] <- ']';
  line.[3] <- ' ';
  spinner_idx := (!spinner_idx + 1) mod (Array.length spinner);
  let percent' = Printf.sprintf "%3d%%" percent in
  String.blit percent' 0 line (progress_bar_width + 4) 4;
  Printf.printf "\r%s%!" line;
  if percent = 100 then Printf.printf "\n"

let mib = Int64.mul 1024L 1024L

let info filename filter =
  let t =
    let open Lwt in
    Lwt_unix.openfile filename [ Lwt_unix.O_RDONLY ] 0
    >>= fun fd ->
    let buffer = Cstruct.create 1024 in
    Lwt_cstruct.complete (Lwt_cstruct.read fd) buffer
    >>= fun () ->
    let h, _ = expect_ok (Header.read buffer) in
    let original_sexp = Header.sexp_of_t h in
    let sexp = match filter with
      | None -> original_sexp
      | Some str -> Sexplib.Path.get ~str original_sexp in
    Printf.printf "%s\n" (Sexplib.Sexp.to_string_hum sexp);
    return (`Ok ()) in
  Lwt_main.run t

let write filename sector data trace =
  let block =
     if trace
     then (module TracedBlock: BLOCK)
     else (module Block: BLOCK) in
  let module BLOCK = (val block: BLOCK) in
  let module B = Qcow.Make(BLOCK) in
  let t =
    let open Lwt in
    BLOCK.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.connect x
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to read qcow formatted data on %s" filename)
      | `Ok x ->
        let npages = (String.length data + 4095) / 4096 in
        let buf = Io_page.(to_cstruct (get npages)) in
        Cstruct.memset buf 0;
        Cstruct.blit_from_string data 0 buf 0 (String.length data);
        B.write x sector [ buf ]
        >>= function
        | `Error _ -> failwith "write failed"
        | `Ok () -> return (`Ok ()) in
  Lwt_main.run t

let read filename sector length trace =
  let block =
     if trace
     then (module TracedBlock: BLOCK)
     else (module Block: BLOCK) in
  let module BLOCK = (val block: BLOCK) in
  let module B = Qcow.Make(BLOCK) in
  let t =
    let open Lwt in
    BLOCK.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.connect x
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to read qcow formatted data on %s" filename)
      | `Ok x ->
        let length = Int64.to_int length * 512 in
        let npages = (length + 4095) / 4096 in
        let buf = Io_page.(to_cstruct (get npages)) in
        B.read x sector [ buf ]
        >>= function
        | `Error _ -> failwith "write failed"
        | `Ok () ->
          let result = Cstruct.sub buf 0 length in
          Printf.printf "%s%!" (Cstruct.to_string result);
          return (`Ok ()) in
  Lwt_main.run t

let check filename =
  let module B = Qcow.Make(Block) in
  let open Lwt in
  let t =
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.connect x
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to read qcow formatted data on %s" filename)
      | `Ok x ->
        Mirage_block.fold_s ~f:(fun acc ofs buf ->
          return ()
        ) () (module B) x
        >>= function
        | `Error (`Msg m) -> failwith m
        | `Ok () ->
          return (`Ok ()) in
  Lwt_main.run t

exception Non_zero

(* slow but performance is not a concern *)
let is_zero buffer =
  try
    for i = 0 to Cstruct.len buffer - 1 do
      if Cstruct.get_uint8 buffer i <> 0 then raise Non_zero
    done;
    true
  with Non_zero -> false

let discard unsafe_buffering filename =
  let block =
     if unsafe_buffering
     then (module UnsafeBlock: BLOCK)
     else (module Block: BLOCK) in
  let module BLOCK = (val block: BLOCK) in
  let module B = Qcow.Make(BLOCK) in
  let open Lwt in
  let t =
    BLOCK.connect filename
    >>*= fun x ->
    BLOCK.get_info x
    >>= fun info ->
    B.connect x
    >>*= fun x ->
    Mirage_block.fold_mapped_s
      (fun acc sector buffer ->
        if is_zero buffer then begin
          let len = Cstruct.len buffer in
          assert (len mod info.BLOCK.sector_size = 0);
          let n = Int64.of_int @@ len / info.BLOCK.sector_size in
          if Int64.add sector n = info.BLOCK.size_sectors then begin
            (* The last block in the file: this is our last chance to discard *)
            let sector = match acc with None -> sector | Some x -> x in
            let n = Int64.sub info.BLOCK.size_sectors sector in
            B.discard x ~sector ~n ()
            >>*= fun () ->
            Lwt.return (`Ok None)
          end else begin
            (* start/extend the current zero region *)
            let acc = match acc with None -> Some sector | Some x -> Some x in
            Lwt.return (`Ok acc)
          end
        end else begin
          match acc with
          | Some start ->
            (* we accumulated zeros: discard them now *)
            let n = Int64.sub sector start in
            B.discard x ~sector:start ~n ()
            >>*= fun () ->
            Lwt.return (`Ok None)
          | None ->
            Lwt.return (`Ok None)
        end
      ) None (module B) x
    >>*= fun _ ->
    return (`Ok ()) in
  Lwt_main.run (t >>= fun r -> return (to_cmdliner_error r))

let compact common_options_t unsafe_buffering filename =
  handle_common common_options_t;
  let block =
     if unsafe_buffering
     then (module UnsafeBlock: BLOCK)
     else (module Block: BLOCK) in
  let module BLOCK = (val block: BLOCK) in
  let module B = Qcow.Make(BLOCK) in
  let open Lwt in
  let progress_cb = if common_options_t.progress then Some progress_cb else None in
  let t =
    BLOCK.connect filename
    >>*= fun x ->
    B.connect x
    >>*= fun x ->
    B.get_info x
    >>= fun info ->
    B.compact x ?progress_cb ()
    >>*= fun report ->
    if report.B.old_size = report.B.new_size
    then Printf.printf "I couldn't make the file any smaller. Consider running `discard`.\n"
    else begin
      let smaller_sectors = Int64.sub report.B.old_size report.B.new_size in
      let sector_size = Int64.of_int info.B.sector_size in
      let smaller_mib = Int64.(div (mul smaller_sectors sector_size) mib) in
      Printf.printf "The file is now %Ld MiB smaller.\n" smaller_mib
    end;
    B.Debug.check_no_overlaps x
    >>*= fun () ->
    return (`Ok ()) in
  Lwt_main.run (t >>= fun r -> return (to_cmdliner_error r))

let repair unsafe_buffering filename =
  let block =
     if unsafe_buffering
     then (module UnsafeBlock: BLOCK)
     else (module Block: BLOCK) in
  let module BLOCK = (val block: BLOCK) in
  let module B = Qcow.Make(BLOCK) in
  let open Lwt in
  let t =
    BLOCK.connect filename
    >>*= fun x ->
    B.connect x
    >>*= fun x ->
    B.rebuild_refcount_table x
    >>*= fun () ->
    B.Debug.check_no_overlaps x
    >>*= fun () ->
    return (`Ok ()) in
  Lwt_main.run (t >>= fun r -> return (to_cmdliner_error r))

let decode filename output =
  let module B = Qcow.Make(Block) in
  let open Lwt in
  let t =
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.connect x
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to read qcow formatted data on %s" filename)
      | `Ok x ->
        B.get_info x
        >>= fun info ->
        let total_size = Int64.(mul info.B.size_sectors (of_int info.B.sector_size)) in
        Lwt_unix.openfile output [ Lwt_unix.O_WRONLY; Lwt_unix.O_CREAT ] 0o0644
        >>= fun fd ->
        Lwt_unix.LargeFile.ftruncate fd total_size
        >>= fun () ->
        Lwt_unix.close fd
        >>= fun () ->
        Block.connect output
        >>= function
        | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
        | `Ok y ->
          Mirage_block.sparse_copy (module B) x (module Block) y
          >>= function
          | `Error _ -> failwith "copy failed"
          | `Ok () -> return (`Ok ()) in
  Lwt_main.run t

let encode filename output =
  let module B = Qcow.Make(Block) in
  let open Lwt in
  let t =
    Block.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok raw_input ->
      Block.get_info raw_input
      >>= fun raw_input_info ->
      let total_size = Int64.(mul raw_input_info.Block.size_sectors (of_int raw_input_info.Block.sector_size)) in
      Lwt_unix.openfile output [ Lwt_unix.O_WRONLY; Lwt_unix.O_CREAT ] 0o0644
      >>= fun fd ->
      Lwt_unix.close fd
      >>= fun () ->
      Block.connect output
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to open %s" output)
      | `Ok raw_output ->
        B.create raw_output ~size:total_size ()
        >>= function
        | `Error _ -> failwith (Printf.sprintf "Failed to create qcow formatted data on %s" output)
        | `Ok qcow_output ->

          Mirage_block.sparse_copy (module Block) raw_input (module B) qcow_output
          >>= function
          | `Error (`Msg m) -> failwith m
          | `Error _ -> failwith "copy failed"
          | `Ok () -> return (`Ok ()) in
  Lwt_main.run t

let create size strict_refcounts trace filename =
  let block =
     if trace
     then (module TracedBlock: BLOCK)
     else (module Block: BLOCK) in
  let module BLOCK = (val block: BLOCK) in
  let module B = Qcow.Make(BLOCK) in
  let open Lwt in
  let t =
    Lwt_unix.openfile filename [ Lwt_unix.O_CREAT ] 0o0644
    >>= fun fd ->
    Lwt_unix.close fd
    >>= fun () ->
    BLOCK.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok x ->
      B.create x ~size ~lazy_refcounts:(not strict_refcounts) ()
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to create qcow formatted data on %s" filename)
      | `Ok x -> return (`Ok ()) in
  Lwt_main.run t

let resize trace filename new_size ignore_data_loss =
  let block =
     if trace
     then (module TracedBlock: BLOCK)
     else (module Block: BLOCK) in
  let module BLOCK = (val block: BLOCK) in
  let module B = Qcow.Make(BLOCK) in
  let open Lwt in
  let t =
    BLOCK.connect filename
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to open %s" filename)
    | `Ok block ->
    B.connect block
    >>= function
    | `Error _ -> failwith (Printf.sprintf "Failed to read qcow-formatted metadata on %s" filename)
    | `Ok qcow ->
    B.get_info qcow
    >>= fun info ->
    let data_loss =
      let existing_size = Int64.(mul info.B.size_sectors (of_int info.B.sector_size)) in
      existing_size > new_size in
    if not ignore_data_loss && data_loss
    then return (`Error(false, "Making a disk smaller results in data loss:\ndisk is currently %Ld bytes which is larger than requested %Ld\n.Please see the --ignore-data-loss option."))
    else begin
      B.resize qcow ~new_size ~ignore_data_loss ()
      >>= function
      | `Error _ -> failwith (Printf.sprintf "Failed to resize qcow formatted data on %s" filename)
      | `Ok x -> return (`Ok ())
    end in
  Lwt_main.run t

type output = [
  | `Text
  | `Json
]

let is_zero buf =
  let rec loop ofs =
    (ofs >= Cstruct.len buf) || (Cstruct.get_uint8 buf ofs = 0 && (loop (ofs + 1))) in
  loop 0

let mapped filename format ignore_zeroes =
  let module B = Qcow.Make(Block) in
  let open Lwt in
  let t =
    Block.connect filename
    >>*= fun x ->
    B.connect x
    >>*= fun x ->
    B.get_info x
    >>= fun info ->
    Printf.printf "# offset (bytes), length (bytes)\n";
    Mirage_block.fold_mapped_s ~f:(fun acc sector_ofs data ->
      let sector_bytes = Int64.(mul sector_ofs (of_int info.B.sector_size)) in
      if not ignore_zeroes || not(is_zero data)
      then Printf.printf "%Lx %d\n" sector_bytes (Cstruct.len data);
      Lwt.return (`Ok ())
    ) () (module B) x
    >>*= fun () ->
    return (`Ok ()) in
  Lwt_main.run (t >>= fun r -> return (to_cmdliner_error r))
