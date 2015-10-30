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
open Error

module Make(B: V1_LWT.BLOCK) = struct

  type 'a io = 'a Lwt.t
  type error = B.error
  type info = {
    read_write : bool;
    sector_size : int;
    size_sectors : int64;
  }

  type id = B.id
  type page_aligned_buffer = B.page_aligned_buffer
  type t = {
    h: Header.t;
    base: B.t;
    base_info: B.info;
    info: info
  }

  let get_info t = Lwt.return t.info

  let (>>*=) m f =
    let open Lwt in
    m >>= function
    | `Error x -> Lwt.return (`Error x)
    | `Ok x -> f x

  let rec iter f = function
    | [] -> Lwt.return (`Ok ())
    | x :: xs ->
        f x >>*= fun () ->
        iter f xs

  (* Return data at [offset] in the underlying block device, up to the sector
     boundary. This is useful for fields which don't cross boundaries *)
  let read_field t offset =
    let sector = Int64.(div offset (of_int t.base_info.B.sector_size)) in
    let buf = Cstruct.sub Io_page.(to_cstruct (get 1)) 0 t.base_info.B.sector_size in
    B.read t.base sector [ buf ]
    >>*= fun () ->
    let within = Int64.(to_int (rem offset (of_int t.base_info.B.sector_size))) in
    Lwt.return (`Ok (Cstruct.shift buf within))

  (* An address in a qcow image is broken into 3 levels: *)
  type address = {
    l1_index: int64; (* index in the L1 table *)
    l2_index: int64; (* index in the L2 table *)
    cluster: int64;  (* index within the cluster *)
  } with sexp
  let address_of_offset cluster_bits x =
    let l2_bits = cluster_bits - 3 in
    let ( <| ) = Int64.shift_left and ( |> ) = Int64.shift_right_logical in
    let l1_index = x |> (l2_bits + cluster_bits) in
    let l2_index = (x <| (64 - l2_bits - cluster_bits)) |> (64 - l2_bits) in
    let cluster  = (x <| (64 - cluster_bits)) |> (64 - cluster_bits) in
    { l1_index; l2_index; cluster }

  let lookup t a =
    let table_offset = t.h.Header.l1_table_offset in
    let l2_bits = Int32.to_int t.h.Header.cluster_bits - 3 in
    (* Read l1[l1_index] as a 64-bit offset *)
    let l1_index_offset = Int64.(add t.h.Header.l1_table_offset (mul 8L a.l1_index))in
    read_field t l1_index_offset
    >>*= fun buf ->
    let l2_table_offset = Cstruct.LE.get_uint64 buf 0 in
    let l2_index_offset = Int64.(add l2_table_offset (mul 8L a.l2_index)) in
    read_field t l2_index_offset
    >>*= fun buf ->
    let cluster_offset = Cstruct.LE.get_uint64 buf 0 in
    Lwt.return (`Ok (Int64.add cluster_offset a.cluster))

  (* Decompose into single sector reads *)
  let rec chop into ofs = function
    | [] -> []
    | buf :: bufs ->
      if Cstruct.len buf > into then begin
        let this = ofs, Cstruct.sub buf 0 into in
        let rest = chop into (Int64.succ ofs) (Cstruct.shift buf into :: bufs) in
        this :: rest
      end else begin
        (ofs, buf) :: (chop into (Int64.succ ofs) bufs)
      end

  let read t sector bufs =
    (* Inefficiently perform 3x physical I/Os for every 1 virtual I/O *)
    iter (fun (sector, buf) ->
      let offset = Int64.mul sector 512L in
      let address = address_of_offset (Int32.to_int t.h.Header.cluster_bits) offset in
      lookup t address
      >>*= fun offset' ->
      let base_sector = Int64.(div offset' (of_int t.base_info.B.sector_size)) in
      B.read t.base base_sector [ buf ]
    ) (chop t.base_info.B.sector_size sector bufs)
  let write t ofs bufs = Lwt.return (`Error `Unimplemented)

  let disconnect t = B.disconnect t.base

  let connect base =
    let open Lwt in
    B.get_info base
    >>= fun base_info ->
    let sector = Cstruct.sub Io_page.(to_cstruct (get 1)) 0 base_info.B.sector_size in
    B.read base 0L [ sector ]
    >>= function
    | `Error x -> Lwt.return (`Error x)
    | `Ok () ->
      match Header.read sector with
      | Error (`Msg m) -> Lwt.return (`Error (`Unknown m))
      | Ok (h, _) ->
        let size_sectors = Int64.(div h.Header.size (of_int base_info.B.sector_size)) in
        let info' = {
          read_write = false;
          sector_size = 512;
          size_sectors = size_sectors
        } in
        Lwt.return (`Ok { h; base; info = info'; base_info })
end
