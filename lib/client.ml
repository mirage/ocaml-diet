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

module Make(B: S.RESIZABLE_BLOCK) = struct

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
    info: info;
    mutable next_cluster: int64;
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
  
  let ( <| ) = Int64.shift_left
  let ( |> ) = Int64.shift_right_logical

  let address_of_offset cluster_bits x =
    let l2_bits = cluster_bits - 3 in
    let l1_index = x |> (l2_bits + cluster_bits) in
    let l2_index = (x <| (64 - l2_bits - cluster_bits)) |> (64 - l2_bits) in
    let cluster  = (x <| (64 - cluster_bits)) |> (64 - cluster_bits) in
    { l1_index; l2_index; cluster }

  type offset = {
    offset: int64;
    copied: bool; (* refcount = 1 implies no snapshots implies direct write ok *)
    compressed: bool;
  }

  (* L1 and L2 entries have a "copied" bit which, if set, means that
     the block isn't shared and therefore can be written to directly *)
  let get_copied x = x |> 63 = 1L

  (* L2 entries can be marked as compressed *)
  let get_compressed x = (x <| 1) |> 63 = 1L

  let parse_offset buf =
    let raw = Cstruct.BE.get_uint64 buf 0 in
    let copied = raw |> 63 = 1L in
    let compressed = (raw <| 1) |> 63 = 1L in
    let offset = (raw <| 2) |> 2 in
    { offset; copied; compressed }

  let lookup t a =
    let table_offset = t.h.Header.l1_table_offset in
    (* Read l1[l1_index] as a 64-bit offset *)
    let l1_index_offset = Int64.(add t.h.Header.l1_table_offset (mul 8L a.l1_index))in
    read_field t l1_index_offset
    >>*= fun buf ->
    let l2_table_offset = Cstruct.BE.get_uint64 buf 0 in
    let compressed = get_compressed l2_table_offset in
    if compressed then failwith "compressed";
    (* mask off the copied and compressed bits *)
    let l2_table_offset = (l2_table_offset <| 2) |> 2 in
    if l2_table_offset = 0L
    then Lwt.return (`Ok None)
    else
      let l2_index_offset = Int64.(add l2_table_offset (mul 8L a.l2_index)) in
      read_field t l2_index_offset
      >>*= fun buf ->
      let cluster_offset = Cstruct.BE.get_uint64 buf 0 in
      let compressed = get_compressed cluster_offset in
      if compressed then failwith "compressed";
      (* mask off the copied and compressed bits *)
      let cluster_offset = (cluster_offset <| 2) |> 2 in
      if cluster_offset = 0L
      then Lwt.return (`Ok None)
      else Lwt.return (`Ok (Some (Int64.add cluster_offset a.cluster)))

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
      >>*= function
      | None ->
        Cstruct.memset buf 0;
        Lwt.return (`Ok ())
      | Some offset' ->
        let base_sector = Int64.(div offset' (of_int t.base_info.B.sector_size)) in
        B.read t.base base_sector [ buf ]
    ) (chop t.base_info.B.sector_size sector bufs)
  let write t ofs bufs = Lwt.return (`Error `Unimplemented)

  let disconnect t = B.disconnect t.base

  let make base h =
    let open Lwt in
    B.get_info base
    >>= fun base_info ->
    (* The virtual disk has 512 byte sectors *)
    let info' = {
      read_write = false;
      sector_size = 512;
      size_sectors = Int64.(div h.Header.size 512L);
    } in
    (* We assume the backing device is resized dynamically so the
       size is the address of the next cluster *)
    let size_bytes = Int64.(mul base_info.B.size_sectors (of_int base_info.B.sector_size)) in
    let next_cluster = Int64.(div size_bytes (1L <| (Int32.to_int h.Header.cluster_bits))) in
    Lwt.return (`Ok { h; base; info = info'; base_info; next_cluster })

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
      | Ok (h, _) -> make base h

  let malloc_cluster t =
    let cluster_bits = Int32.to_int t.Header.cluster_bits in
    let npages = max 1 (cluster_bits lsl (cluster_bits - 12)) in
    let pages = Io_page.(to_cstruct (get npages)) in
    Cstruct.sub pages 0 (1 lsl cluster_bits)

  (* The number of 16-bit reference counts per cluster *)
  let refcounts_per_cluster t =
    let cluster_bits = Int32.to_int t.Header.cluster_bits in
    let size = t.Header.size in
    let cluster_size = 1L <| cluster_bits in
    (* Each reference count is 2 bytes long *)
    Int64.div cluster_size 2L

  let round_up x size = Int64.(mul (div (add x (pred size)) size) size)

  (* Compute the maximum size of the refcount table, if we need it *)
  let max_refount_table_size t =
    let cluster_bits = Int32.to_int t.h.Header.cluster_bits in
    let size = t.h.Header.size in
    let cluster_size = 1L <| cluster_bits in
    let refs_per_cluster = refcounts_per_cluster t.h in
    let size_in_clusters = Int64.div (round_up size cluster_size) cluster_size in
    let refs_clusters_required = Int64.div (round_up size_in_clusters refs_per_cluster) refs_per_cluster in
    (* Each cluster containing references consumes 8 bytes in the
       refcount_table. How much space is that? *)
    let refcount_table_bytes = Int64.mul refs_clusters_required 8L in
    Int64.div (round_up refcount_table_bytes cluster_size) cluster_size

  (** Allocate a cluster, resize the underlying device and return the
      byte offset of the new cluster, suitable for writing to one of
      the various metadata tables. *)
  let allocate_cluster t =
    let cluster = t.next_cluster in
    t.next_cluster <- Int64.succ t.next_cluster;
    let new_size_bytes = Int64.mul t.next_cluster (1L <| (Int32.to_int t.h.Header.cluster_bits)) in
    let new_size_sectors = Int64.(div new_size_bytes (of_int t.base_info.B.sector_size)) in
    B.resize t.base new_size_sectors
    >>*= fun () ->
    let offset = Int64.mul cluster (1L <| (Int32.to_int t.h.Header.cluster_bits)) in
    Lwt.return (`Ok offset)

  let incr_refcount t cluster =
    let cluster_bits = Int32.to_int t.h.Header.cluster_bits in
    let size = t.h.Header.size in
    let cluster_size = 1L <| cluster_bits in
    let index_in_cluster = Int64.(to_int (div cluster (refcounts_per_cluster t.h))) in
    let within_cluster = Int64.(to_int (rem cluster (refcounts_per_cluster t.h))) in
    if index_in_cluster > 0
    then Lwt.return (`Error (`Unknown "I don't know how to enlarge a refcount table yet"))
    else begin
      let cluster = malloc_cluster t.h in
      let refcount_table_sector = Int64.(div t.h.Header.refcount_table_offset (of_int t.base_info.B.sector_size)) in
      B.read t.base refcount_table_sector [ cluster ]
      >>*= fun () ->
      let offset = Cstruct.BE.get_uint64 cluster index_in_cluster in
      if offset = 0L then begin
        allocate_cluster t
        >>*= fun offset ->
        let cluster' = malloc_cluster t.h in
        Cstruct.memset cluster' 0;
        Cstruct.BE.set_uint16 cluster' within_cluster 1;
        let sector = Int64.(div offset (of_int t.base_info.B.sector_size)) in
        B.write t.base sector [ cluster' ]
        >>*= fun () ->
        Cstruct.BE.set_uint64 cluster index_in_cluster offset;
        B.write t.base refcount_table_sector [ cluster ]
        >>*= fun () ->
        (* recursively increment refcunt of offset? *)
        Lwt.return (`Ok ())
      end else begin
        B.read t.base offset [ cluster ]
        >>*= fun () ->
        let count = Cstruct.BE.get_uint16 cluster within_cluster in
        Cstruct.BE.set_uint16 cluster within_cluster (count + 1);
        let sector = Int64.(div offset (of_int t.base_info.B.sector_size)) in
        B.write t.base sector [ cluster ]
      end
    end

  let create base size =
    let version = `Two in
    let backing_file_offset = 0L in
    let backing_file_size = 0l in
    let cluster_bits = 16 in
    let cluster_size = 1L <| cluster_bits in
    let crypt_method = `None in
    (* qemu-img places the refcount table next in the file and only
       qemu-img creates a tiny refcount table and grows it on demand *)
    let refcount_table_offset = cluster_size in
    let refcount_table_clusters = 1L in

    (* qemu-img places the L1 table after the refcount table *)
    let l1_table_offset = Int64.(mul (add 1L refcount_table_clusters) (1L <| cluster_bits)) in
    (* The L2 table is of size (1L <| cluster_bits) bytes
       and contains (1L <| (cluster_bits - 3)) 8-byte pointers.
       A single L2 table therefore manages
       (1L <| (cluster_bits - 3)) * (1L <| cluster_bits) bytes
       = (1L <| (2 * cluster_bits - 3)) bytes. *)
    let bytes_per_l2 = 1L <| (2 * cluster_bits - 3) in
    let l2_tables_required = Int64.div (round_up size bytes_per_l2) bytes_per_l2 in
    let nb_snapshots = 0l in
    let snapshots_offset = 0L in
    let h = {
      Header.version; backing_file_offset; backing_file_size;
      cluster_bits = Int32.of_int cluster_bits; size; crypt_method;
      l1_size = Int64.to_int32 l2_tables_required;
      l1_table_offset; refcount_table_offset;
      refcount_table_clusters = Int64.to_int32 refcount_table_clusters;
      nb_snapshots; snapshots_offset
    } in
    (* Resize the underlying device to contain the header + refcount table
       + l1 table. Future allocations will enlarge the file. *)
    let l1_size_bytes = Int64.mul 8L l2_tables_required in
    let next_free_byte = round_up (Int64.add l1_table_offset l1_size_bytes) cluster_size in
    let open Lwt in
    B.get_info base
    >>= fun base_info ->
    let size_sectors = Int64.(div next_free_byte (of_int base_info.B.sector_size)) in
    B.resize base size_sectors
    >>*= fun () ->

    let page = Io_page.(to_cstruct (get 1)) in
    match Header.write h page with
    | Result.Ok _ ->
      B.write base 0L [ page ]
      >>*= fun () ->
      make base h
      >>*= fun t ->
      (* Write an initial empty refcount table *)
      let cluster = malloc_cluster t.h in
      Cstruct.memset cluster 0;
      B.write base Int64.(div refcount_table_offset (of_int t.base_info.B.sector_size)) [ cluster ]
      >>*= fun () ->
      incr_refcount t 0L (* header *)
      >>*= fun () ->
      incr_refcount t (Int64.div refcount_table_offset cluster_size)
      >>*= fun () ->
      (* Write an initial empty L1 table *)
      B.write base Int64.(div l1_table_offset (of_int t.base_info.B.sector_size)) [ cluster ]
      >>*= fun () ->
      incr_refcount t (Int64.div l1_table_offset cluster_size)
      >>*= fun () ->
      Lwt.return (`Ok t)
    | Result.Error (`Msg m) ->
      Lwt.return (`Error (`Unknown m))
end
