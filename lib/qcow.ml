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
open Qcow_error
open Qcow_types
module Error = Qcow_error
module Header = Qcow_header
module Virtual = Qcow_virtual
module Physical = Qcow_physical

let ( <| ) = Int64.shift_left
let ( |> ) = Int64.shift_right_logical

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

module Make(B: Qcow_s.RESIZABLE_BLOCK) = struct

  type 'a io = 'a Lwt.t
  type error = B.error
  type info = {
    read_write : bool;
    sector_size : int;
    size_sectors : int64;
  }

  type id = B.id
  type page_aligned_buffer = B.page_aligned_buffer

  let (>>*=) m f =
    let open Lwt in
    m >>= function
    | `Error x -> Lwt.return (`Error x)
    | `Ok x -> f x

  (* Run all threads in parallel, wait for all to complete, then iterate through
     the results and return the first failure we discover. *)
  let rec iter_p f xs =
    let open Lwt in
    let threads = List.map f xs in
    Lwt_list.fold_left_s (fun acc t ->
        t >>= fun result ->
        match acc with
        | `Error x -> Lwt.return (`Error x) (* first error wins *)
        | `Ok () -> t
      ) (`Ok ()) threads

  module Int64Map = Map.Make(Int64)
  module Int64Set = Set.Make(Int64)

  module ClusterCache = struct
    type t = {
      read_cluster: int64 -> [ `Ok of Cstruct.t | `Error of error ] Lwt.t;
      write_cluster: int64 -> Cstruct.t -> [ `Ok of unit | `Error of error ] Lwt.t;
      flush: unit -> [ `Ok of unit | `Error of error ] Lwt.t;
      mutable clusters: Cstruct.t Int64Map.t;
      mutable locked: Int64Set.t;
      m: Lwt_mutex.t;
      c: unit Lwt_condition.t;
    }
    let make ~read_cluster ~write_cluster ~flush () =
      let m = Lwt_mutex.create () in
      let c = Lwt_condition.create () in
      let clusters = Int64Map.empty in
      let locked = Int64Set.empty in
      { read_cluster; write_cluster; flush; m; c; clusters; locked }

    let with_lock t cluster f =
      let open Lwt in
      let lock cluster =
        Lwt_mutex.with_lock t.m
          (fun () ->
             let rec loop () =
               if Int64Set.mem cluster t.locked then begin
                 Lwt_condition.wait t.c ~mutex:t.m
                 >>= fun () ->
                 loop ()
               end else begin
                 t.locked <- Int64Set.add cluster t.locked;
                 Lwt.return ()
               end in
             loop ()
          ) in
      let unlock cluster =
        Lwt_mutex.with_lock t.m
          (fun () ->
             t.locked <- Int64Set.remove cluster t.locked;
             Lwt.return ()
          )
        >>= fun () ->
        Lwt_condition.signal t.c ();
        Lwt.return () in
      Lwt.catch
        (fun () ->
           lock cluster >>= fun () ->
           f () >>= fun r ->
           unlock cluster >>= fun () ->
           Lwt.return r)
        (fun e ->
           unlock cluster >>= fun () ->
           Lwt.fail e)
    let read t cluster f =
      let open Lwt in
      with_lock t cluster
        (fun () ->
           ( if Int64Map.mem cluster t.clusters
             then Lwt.return (`Ok (Int64Map.find cluster t.clusters))
             else begin
               t.read_cluster cluster
               >>*= fun buf ->
               t.clusters <- Int64Map.add cluster buf t.clusters;
               Lwt.return (`Ok buf)
             end
           ) >>*= fun buf ->
           f buf
        )
    let update t cluster f =
      let open Lwt in
      with_lock t cluster
        (fun () ->
           ( if Int64Map.mem cluster t.clusters
             then Lwt.return (`Ok (Int64Map.find cluster t.clusters))
             else begin
               t.read_cluster cluster
               >>*= fun buf ->
               Lwt.return (`Ok buf)
             end
           ) >>*= fun buf ->
           f buf
           >>*= fun () ->
           t.clusters <- Int64Map.add cluster buf t.clusters;
           t.write_cluster cluster buf
        )
      >>*= fun () ->
      t.flush ()
  end


  type t = {
    mutable h: Header.t;
    base: B.t;
    base_info: B.info;
    info: info;
    mutable next_cluster: int64;
    next_cluster_m: Lwt_mutex.t;
    cache: ClusterCache.t;
    (* for convenience *)
    cluster_bits: int;
    sector_size: int;
    mutable lazy_refcounts: bool; (* true if we are omitting refcounts right now *)
  }

  let get_info t = Lwt.return t.info

  (* Another way to achieve this would be to create a virtual block device
     with a little bit of padding on the end *)
  let read_base base base_sector buf =
    (* Qemu-img will 'allocate' the last cluster by writing only the last sector.
       Cope with this by assuming all later sectors are full of zeroes *)
    let open Lwt in
    B.get_info base
    >>= fun base_info ->
    let buf_len = Int64.of_int (Cstruct.len buf) in
    let missing_sectors =
      Int64.sub
        Int64.(add base_sector (div buf_len (of_int base_info.B.sector_size)))
        base_info.B.size_sectors in
    if missing_sectors > 0L then begin
      let available_sectors = Int64.(sub (div buf_len (of_int base_info.B.sector_size)) missing_sectors) in
      let bytes = Int64.(to_int (mul available_sectors (of_int base_info.B.sector_size))) in
      B.read base base_sector [ Cstruct.sub buf 0 bytes ]
      >>*= fun () ->
      Cstruct.(memset (shift buf bytes) 0);
      Lwt.return (`Ok ())
    end else B.read base base_sector [ buf ]

  let malloc t =
    let cluster_bits = Int32.to_int t.Header.cluster_bits in
    let npages = max 1 (cluster_bits lsl (cluster_bits - 12)) in
    let pages = Io_page.(to_cstruct (get npages)) in
    Cstruct.sub pages 0 (1 lsl cluster_bits)

  (* Mmarshal a disk physical address written at a given offset within the disk. *)
  let marshal_physical_address t offset v =
    let cluster, within = Physical.to_cluster ~cluster_bits:t.cluster_bits offset in
    ClusterCache.update t.cache cluster
      (fun buf ->
         match Physical.write v (Cstruct.shift buf within) with
         | Error (`Msg m) -> Lwt.return (`Error (`Unknown m))
         | Ok _ -> Lwt.return (`Ok ())
      )

  (* Unmarshal a disk physical address written at a given offset within the disk. *)
  let unmarshal_physical_address t offset =
    let cluster, within = Physical.to_cluster ~cluster_bits:t.cluster_bits offset in
    ClusterCache.read t.cache cluster
      (fun buf ->
         match Physical.read (Cstruct.shift buf within) with
         | Error (`Msg m) -> Lwt.return (`Error (`Unknown m))
         | Ok (x, _) -> Lwt.return (`Ok x)
      )

  let update_header t h =
    let page = Io_page.(to_cstruct (get 1)) in
    match Header.write h page with
    | Result.Ok _ ->
      B.write t.base 0L [ page ]
      >>*= fun () ->
      B.flush t.base
      >>*= fun () ->
      Log.debug (fun f -> f "Written header");
      t.h <- h;
      Lwt.return (`Ok ())
    | Result.Error (`Msg m) ->
      Lwt.return (`Error (`Unknown m))

  let resize_base base sector_size new_size =
    let sector, within = Physical.to_sector ~sector_size new_size in
    if within <> 0
    then Lwt.return (`Error (`Unknown (Printf.sprintf "Internal error: attempting to resize to a non-sector multiple %s" (Physical.to_string new_size))))
    else begin
      B.resize base sector
      >>*= fun () ->
      Log.debug (fun f -> f "Resized device to %Ld sectors of size %d" (Qcow_physical.to_bytes new_size) sector_size);
      Lwt.return (`Ok ())
    end

  module Cluster = struct

    (** Allocate contiguous clusters, increasing the size of the underying device.
        This must be called with next_cluster_m held, and the mutex must not be
        released until the allocation has been persisted so that concurrent threads
        will not allocate another cluster for the same purpose. *)
    let allocate_clusters t n =
      let cluster = t.next_cluster in
      t.next_cluster <- Int64.add t.next_cluster n;
      Log.debug (fun f -> f "Soft allocated span of clusters from %Ld (length %Ld)" cluster n);
      resize_base t.base t.sector_size (Physical.make (t.next_cluster <| t.cluster_bits))
      >>*= fun () ->
      Lwt.return (`Ok cluster)

    module Refcount = struct
      (* The refcount table contains pointers to clusters which themselves
         contain the 2-byte refcounts *)
      let read t cluster =
        let within_table = Int64.(div cluster (Header.refcounts_per_cluster t.h)) in
        let within_cluster = Int64.(to_int (rem cluster (Header.refcounts_per_cluster t.h))) in

        let offset = Physical.make Int64.(add t.h.Header.refcount_table_offset (mul 8L within_table)) in
        unmarshal_physical_address t offset
        >>*= fun offset ->
        if Physical.to_bytes offset = 0L
        then Lwt.return (`Ok 0)
        else begin
          let cluster, _ = Physical.to_cluster ~cluster_bits:t.cluster_bits offset in
          ClusterCache.read t.cache cluster
            (fun buf ->
               Lwt.return (`Ok (Cstruct.BE.get_uint16 buf (2 * within_cluster)))
            )
        end

      (** Increment the refcount of a given cluster. Note this might need
          to allocate itself, to enlarge the refcount table. When this function
          returns the refcount is guaranteed to have been persisted. *)
      let rec really_incr t cluster =
        let within_table = Int64.(div cluster (Header.refcounts_per_cluster t.h)) in
        let within_cluster = Int64.(to_int (rem cluster (Header.refcounts_per_cluster t.h))) in

        (* If the table (containing pointers to clusters which contain the refcounts)
           is too small, then reallocate it now. *)
        let cluster_containing_pointer =
          let within_table_offset = Int64.mul within_table 8L in
          within_table_offset |> t.cluster_bits in
        let current_size_clusters = Int64.of_int32 t.h.Header.refcount_table_clusters in
        ( if cluster_containing_pointer >= current_size_clusters then begin
              let needed = Header.max_refcount_table_size t.h in
              (* Make sure this is actually an increase: make the table 2x larger if not *)
              let needed =
                if needed = current_size_clusters
                then Int64.mul 2L current_size_clusters
                else needed in
              allocate_clusters t needed
              >>*= fun start ->
              (* Copy any existing refcounts into new table *)
              let buf = malloc t.h in
              let rec loop i =
                if i >= Int32.to_int t.h.Header.refcount_table_clusters
                then Lwt.return (`Ok ())
                else begin
                  let physical = Physical.make Int64.(add t.h.Header.refcount_table_offset (of_int (i lsl t.cluster_bits))) in
                  let sector, _ = Physical.to_sector ~sector_size:t.sector_size physical in
                  read_base t.base sector buf
                  >>*= fun () ->
                  let physical = Physical.make Int64.((add start (of_int i)) <| t.cluster_bits) in
                  let sector, _ = Physical.to_sector ~sector_size:t.sector_size physical in
                  B.write t.base sector [ buf ]
                  >>*= fun () ->
                  loop (i + 1)
                end in
              loop 0
              >>*= fun () ->
              Log.debug (fun f -> f "Copied refcounts into new table");
              (* Zero new blocks *)
              Cstruct.memset buf 0;
              let rec loop i =
                if i >= needed
                then Lwt.return (`Ok ())
                else begin
                  let physical = Physical.make Int64.((add start i) <| t.cluster_bits) in
                  let sector, _ = Physical.to_sector ~sector_size:t.sector_size physical in
                  B.write t.base sector [ buf ]
                  >>*= fun () ->
                  loop (Int64.succ i)
                end in
              loop (Int64.of_int32 t.h.Header.refcount_table_clusters)
              >>*= fun () ->
              let h' = { t.h with
                         Header.refcount_table_offset = start <| t.cluster_bits;
                         refcount_table_clusters = Int64.to_int32 needed;
                       } in
              update_header t h'
              >>*= fun () ->
              (* increase the refcount of the clusters we just allocated *)
              let rec loop i =
                if i >= needed
                then Lwt.return (`Ok ())
                else begin
                  really_incr t (Int64.add start i)
                  >>*= fun () ->
                  loop (Int64.succ i)
                end in
              loop 0L
            end else begin
            Lwt.return (`Ok ())
          end )
        >>*= fun () ->

        let offset = Physical.make Int64.(add t.h.Header.refcount_table_offset (mul 8L within_table)) in
        unmarshal_physical_address t offset
        >>*= fun addr ->
        ( if Physical.to_bytes addr = 0L then begin
              allocate_clusters t 1L
              >>*= fun cluster ->
              (* NB: the pointers in the refcount table are different from the pointers
                 in the cluster table: the high order bits are not used to encode extra
                 information and wil confuse qemu/qemu-img. *)
              let addr = Physical.make ~is_mutable:false (cluster <| t.cluster_bits) in
              (* zero the cluster *)
              let buf = malloc t.h in
              Cstruct.memset buf 0;
              let sector, _ = Physical.to_sector ~sector_size:t.sector_size addr in
              B.write t.base sector [ buf ]
              >>*= fun () ->
              (* Ensure the new zeroed cluster has been persisted before we reference
                 it via `marshal_physical_address` *)
              B.flush t.base
              >>*= fun () ->
              Log.debug (fun f -> f "Allocated new refcount cluster %Ld" cluster);
              marshal_physical_address t offset addr
              >>*= fun () ->
              really_incr t cluster
              >>*= fun () ->
              Lwt.return (`Ok addr)
            end else Lwt.return (`Ok addr) )
        >>*= fun offset ->
        let refcount_cluster, _ = Physical.to_cluster ~cluster_bits:t.cluster_bits offset in
        ClusterCache.update t.cache refcount_cluster
          (fun buf ->
             let current = Cstruct.BE.get_uint16 buf (2 * within_cluster) in
             (* We don't support refcounts of more than 1 *)
             assert (current == 0);
             Cstruct.BE.set_uint16 buf (2 * within_cluster) (current + 1);
             Lwt.return (`Ok ())
          )
        >>*= fun () ->
        Log.debug (fun f -> f "Incremented refcount of cluster %Ld" cluster);
        Lwt.return (`Ok ())

      (* If the lazy refcounts feature is enabled then don't actually Increment
         the refcounts. *)
      let incr t cluster =
        if t.lazy_refcounts
        then Lwt.return (`Ok ())
        else really_incr t cluster

    end


    let read_l1_table t l1_index =
      let table_offset = t.h.Header.l1_table_offset in
      (* Read l1[l1_index] as a 64-bit offset *)
      let l1_table_start = Physical.make t.h.Header.l1_table_offset in
      let l1_index_offset = Physical.shift l1_table_start (Int64.mul 8L l1_index) in
      unmarshal_physical_address t l1_index_offset
      >>*= fun l2_table_offset ->
      Lwt.return (`Ok l2_table_offset)

    (* Find the first l1_index whose values satisfies [f] *)
    let find_mapped_l1_table t l1_index f =
      let table_offset = t.h.Header.l1_table_offset in
      (* Read l1[l1_index] as a 64-bit offset *)
      let rec loop l1_index =
        if l1_index >= Int64.of_int32 t.h.Header.l1_size
        then Lwt.return (`Ok None)
        else begin
          let l1_table_start = Physical.make t.h.Header.l1_table_offset in
          let l1_index_offset = Physical.shift l1_table_start (Int64.mul 8L l1_index) in

          let cluster, within = Physical.to_cluster ~cluster_bits:t.cluster_bits l1_index_offset in
          ClusterCache.read t.cache cluster
            (fun buf ->
               let rec loop i : [ `Skip of int | `GotOne of int ]=
                 if i >= (Cstruct.len buf) then `Skip (i / 8) else begin
                   if f (Cstruct.BE.get_uint64 buf i)
                   then `GotOne (i / 8)
                   else loop (i + 8)
                 end in
               Lwt.return (`Ok (loop within)))
          >>*= function
          | `GotOne i ->
            Lwt.return (`Ok (Some (Int64.(add l1_index (of_int i)))))
          | `Skip n ->
            loop Int64.(add l1_index (of_int n))
        end in
      loop l1_index

    let write_l1_table t l1_index l2_table_offset =
      let table_offset = t.h.Header.l1_table_offset in
      (* Read l1[l1_index] as a 64-bit offset *)
      let l1_table_start = Physical.make t.h.Header.l1_table_offset in
      let l1_index_offset = Physical.shift l1_table_start (Int64.mul 8L l1_index) in
      marshal_physical_address t l1_index_offset l2_table_offset
      >>*= fun () ->
      Log.debug (fun f -> f "Written l1_table[%Ld] <- %Ld" l1_index (fst @@ Physical.to_cluster ~cluster_bits:t.cluster_bits l2_table_offset));
      Lwt.return (`Ok ())

    let read_l2_table t l2_table_offset l2_index =
      let l2_index_offset = Physical.shift l2_table_offset (Int64.mul 8L l2_index) in
      unmarshal_physical_address t l2_index_offset
      >>*= fun cluster_offset ->
      Lwt.return (`Ok cluster_offset)

    let write_l2_table t l2_table_offset l2_index cluster =
      let l2_index_offset = Physical.shift l2_table_offset (Int64.mul 8L l2_index) in
      marshal_physical_address t l2_index_offset cluster
      >>*= fun _ ->
      Log.debug (fun f -> f "Written l2_table[%Ld] <- %Ld" l2_index (fst @@ Physical.to_cluster ~cluster_bits:t.cluster_bits cluster));
      Lwt.return (`Ok ())

    (* Walk the L1 and L2 tables to translate an address. If a table entry
       is unallocated then return [None]. Note if a [walk_and_allocate] is
       racing with us then we may or may not see the mapping. *)
    let walk_readonly t a =
      read_l1_table t a.Virtual.l1_index
      >>*= fun l2_table_offset ->

      let (>>|=) m f =
        let open Lwt in
        m >>= function
        | `Error x -> Lwt.return (`Error x)
        | `Ok None -> Lwt.return (`Ok None)
        | `Ok (Some x) -> f x in

      (* Look up an L2 table *)
      ( if Physical.to_bytes l2_table_offset = 0L
        then Lwt.return (`Ok None)
        else begin
          if Physical.is_compressed l2_table_offset then failwith "compressed";
          Lwt.return (`Ok (Some l2_table_offset))
        end
      ) >>|= fun l2_table_offset ->

      (* Look up a cluster *)
      read_l2_table t l2_table_offset a.Virtual.l2_index
      >>*= fun cluster_offset ->
      ( if Physical.to_bytes cluster_offset = 0L
        then Lwt.return (`Ok None)
        else begin
          if Physical.is_compressed cluster_offset then failwith "compressed";
          Lwt.return (`Ok (Some cluster_offset))
        end
      ) >>|= fun cluster_offset ->

      Lwt.return (`Ok (Some (Physical.shift cluster_offset a.Virtual.cluster)))

    (* Walk the L1 and L2 tables to translate an address, allocating missing
       entries as we go. *)
    let walk_and_allocate t a =
      Lwt_mutex.with_lock t.next_cluster_m
        (fun () ->
           read_l1_table t a.Virtual.l1_index
           >>*= fun l2_table_offset ->

           (* Look up an L2 table *)
           ( if Physical.to_bytes l2_table_offset = 0L then begin
                 allocate_clusters t 1L
                 >>*= fun cluster ->
                 Refcount.incr t cluster
                 >>*= fun () ->
                 let offset = Physical.make (cluster <| t.cluster_bits) in
                 write_l1_table t a.Virtual.l1_index offset
                 >>*= fun () ->
                 Lwt.return (`Ok offset)
               end else begin
               if Physical.is_compressed l2_table_offset then failwith "compressed";
               Lwt.return (`Ok l2_table_offset)
             end
           ) >>*= fun l2_table_offset ->

           (* Look up a cluster *)
           read_l2_table t l2_table_offset a.Virtual.l2_index
           >>*= fun cluster_offset ->
           ( if Physical.to_bytes cluster_offset = 0L then begin
                 allocate_clusters t 1L
                 >>*= fun cluster ->
                 Refcount.incr t cluster
                 >>*= fun () ->
                 let offset = Physical.make (cluster <| t.cluster_bits) in
                 write_l2_table t l2_table_offset a.Virtual.l2_index offset
                 >>*= fun () ->
                 Lwt.return (`Ok offset)
               end else begin
               if Physical.is_compressed cluster_offset then failwith "compressed";
               Lwt.return (`Ok cluster_offset)
             end
           ) >>*= fun cluster_offset ->

           Lwt.return (`Ok (Physical.shift cluster_offset a.Virtual.cluster))
        )

  end

  (* Starting at byte offset [ofs], map a list of buffers onto a list of
     [byte offset, buffer] pairs, where
       - no [byte offset, buffer] pair crosses an [alignment] boundary;
       - each [buffer] is as large as possible (so for example if we supply
         one large buffer it will only be fragmented to the minimum extent. *)
  let rec chop_into_aligned alignment ofs = function
    | [] -> []
    | buf :: bufs ->
      (* If we're not aligned, sync to the next boundary *)
      let into = Int64.(to_int (sub alignment (rem ofs alignment))) in
      if Cstruct.len buf > into then begin
        let this = ofs, Cstruct.sub buf 0 into in
        let rest = chop_into_aligned alignment Int64.(add ofs (of_int into)) (Cstruct.shift buf into :: bufs) in
        this :: rest
      end else begin
        (ofs, buf) :: (chop_into_aligned alignment Int64.(add ofs (of_int (Cstruct.len buf))) bufs)
      end

  let read t sector bufs =
    let cluster_size = 1L <| t.cluster_bits in
    let byte = Int64.(mul sector (of_int t.info.sector_size)) in
    iter_p (fun (byte, buf) ->
        let vaddr = Virtual.make ~cluster_bits:t.cluster_bits byte in
        Cluster.walk_readonly t vaddr
        >>*= function
        | None ->
          Cstruct.memset buf 0;
          Lwt.return (`Ok ())
        | Some offset' ->
          (* Qemu-img will 'allocate' the last cluster by writing only the last sector.
             Cope with this by assuming all later sectors are full of zeroes *)
          let buf_len = Int64.of_int (Cstruct.len buf) in
          let base_sector, _ = Physical.to_sector ~sector_size:t.sector_size offset' in
          read_base t.base base_sector buf
      ) (chop_into_aligned cluster_size byte bufs)

  let write t sector bufs =
    let cluster_size = 1L <| t.cluster_bits in
    let byte = Int64.(mul sector (of_int t.info.sector_size)) in
    iter_p (fun (byte, buf) ->

        let vaddr = Virtual.make ~cluster_bits:t.cluster_bits byte in
        ( Cluster.walk_readonly t vaddr
          >>*= function
          | None ->
            (* Only the first write to this area needs to allocate, so it's ok
               to make this a little slower *)
            Cluster.walk_and_allocate t vaddr
          | Some offset' ->
            Lwt.return (`Ok offset') )
        >>*= fun offset' ->
        let base_sector, _ = Physical.to_sector ~sector_size:t.sector_size offset' in
        B.write t.base base_sector [ buf ]
        >>*= fun () ->
        Log.debug (fun f -> f "Written user data to cluster %Ld" (fst (Physical.to_cluster ~cluster_bits:t.cluster_bits offset')));
        Lwt.return (`Ok ())
      ) (chop_into_aligned cluster_size byte bufs)

  let seek_mapped t from =
    let bytes = Int64.(mul from (of_int t.sector_size)) in
    let addr = Qcow_virtual.make ~cluster_bits:t.cluster_bits bytes in
    let int64s_per_cluster = 1L <| (Int32.to_int t.h.Header.cluster_bits - 3) in
    let rec scan_l1 a =
      if a.Virtual.l1_index >= Int64.of_int32 t.h.Header.l1_size
      then Lwt.return (`Ok Int64.(mul t.info.size_sectors (of_int t.sector_size)))
      else
        Cluster.find_mapped_l1_table t a.Virtual.l1_index (fun x -> x <> 0L)
        >>*= function
        | None -> Lwt.return (`Ok Int64.(mul t.info.size_sectors (of_int t.sector_size)))
        | Some l1_index ->
          let a = { a with Virtual.l1_index } in
          Cluster.read_l1_table t a.Virtual.l1_index
          >>*= fun x ->
          if Physical.to_bytes x = 0L
          then scan_l1 { a with Virtual.l1_index = Int64.succ a.Virtual.l1_index; l2_index = 0L }
          else
            let rec scan_l2 a =
              if a.Virtual.l2_index >= int64s_per_cluster
              then scan_l1 { a with Virtual.l1_index = Int64.succ a.Virtual.l1_index; l2_index = 0L }
              else
                Cluster.read_l2_table t x a.Virtual.l2_index
                >>*= fun x ->
                if Physical.to_bytes x = 0L
                then scan_l2 { a with Virtual.l2_index = Int64.succ a.Virtual.l2_index }
                else Lwt.return (`Ok (Qcow_virtual.to_offset ~cluster_bits:t.cluster_bits a)) in
            scan_l2 a in
    scan_l1 (Virtual.make ~cluster_bits:t.cluster_bits bytes)
    >>*= fun offset ->
    let x = Int64.(div offset (of_int t.sector_size)) in
    assert (x >= from);
    Lwt.return (`Ok x)

  let seek_unmapped t from =
    let bytes = Int64.(mul from (of_int t.sector_size)) in
    let addr = Qcow_virtual.make ~cluster_bits:t.cluster_bits bytes in
    let int64s_per_cluster = 1L <| (Int32.to_int t.h.Header.cluster_bits - 3) in
    let rec scan_l1 a =
      if a.Virtual.l1_index >= Int64.of_int32 t.h.Header.l1_size
      then Lwt.return (`Ok Int64.(mul t.info.size_sectors (of_int t.sector_size)))
      else
        Cluster.read_l1_table t a.Virtual.l1_index
        >>*= fun x ->
        if Physical.to_bytes x = 0L
        then Lwt.return (`Ok (Qcow_virtual.to_offset ~cluster_bits:t.cluster_bits a))
        else
          let rec scan_l2 a =
            if a.Virtual.l2_index >= int64s_per_cluster
            then scan_l1 { a with Virtual.l1_index = Int64.succ a.Virtual.l1_index; l2_index = 0L }
            else
              Cluster.read_l2_table t x a.Virtual.l2_index
              >>*= fun y ->
              if Physical.to_bytes y = 0L
              then Lwt.return (`Ok (Qcow_virtual.to_offset ~cluster_bits:t.cluster_bits a))
              else scan_l2 { a with Virtual.l2_index = Int64.succ a.Virtual.l2_index} in
          scan_l2 a in
    scan_l1 (Virtual.make ~cluster_bits:t.cluster_bits bytes)
    >>*= fun offset ->
    let x = Int64.(div offset (of_int t.sector_size)) in
    assert (x >= from);
    Lwt.return (`Ok x)

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
    let sector_size = base_info.B.sector_size in
    let cluster_bits = Int32.to_int h.Header.cluster_bits in
    (* The first cluster is allocated after the L1 table *)
    let size_bytes = Int64.(mul base_info.B.size_sectors (of_int sector_size)) in
    let cluster_size = 1L <| cluster_bits in
    (* qemu-img will allocate a cluster by writing only a single sector to the end
       of the file. Therefore we must round up: *)
    let next_cluster = Int64.(div (round_up size_bytes cluster_size) cluster_size) in
    let next_cluster_m = Lwt_mutex.create () in
    let read_cluster i =
      let buf = malloc h in
      let offset = i <| cluster_bits in
      let sector = Int64.(div offset (of_int sector_size)) in
      read_base base sector buf
      >>*= fun () ->
      Lwt.return (`Ok buf) in
    let write_cluster i buf =
      let offset = i <| cluster_bits in
      let sector = Int64.(div offset (of_int sector_size)) in
      B.write base sector [ buf ] in
    let flush () = B.flush base in
    let cache = ClusterCache.make ~read_cluster ~write_cluster ~flush () in
    let lazy_refcounts = match h.Header.additional with Some { Header.lazy_refcounts = true } -> true | _ -> false in
    Lwt.return (`Ok { h; base; info = info'; base_info; next_cluster; next_cluster_m; cache; sector_size; cluster_bits; lazy_refcounts })

  let connect base =
    let open Lwt in
    B.get_info base
    >>= fun base_info ->
    let sector = Cstruct.sub Io_page.(to_cstruct (get 1)) 0 base_info.B.sector_size in
    read_base base 0L sector
    >>= function
    | `Error x -> Lwt.return (`Error x)
    | `Ok () ->
      match Header.read sector with
      | Error (`Msg m) -> Lwt.return (`Error (`Unknown m))
      | Ok (h, _) -> make base h

  let resize t new_size_sectors =
    let size = Int64.mul new_size_sectors 512L in
    let l2_tables_required = Header.l2_tables_required ~cluster_bits:t.cluster_bits size in
    (* Keep it simple for now by refusing resizes which would require us to
       reallocate the L1 table. *)
    let l2_entries_per_cluster = 1L <| (Int32.to_int t.h.Header.cluster_bits - 3) in
    let old_max_entries = Int64.round_up (Int64.of_int32 t.h.Header.l1_size) l2_entries_per_cluster in
    let new_max_entries = Int64.round_up l2_tables_required l2_entries_per_cluster in
    if new_max_entries > old_max_entries
    then Lwt.return (`Error (`Unknown "I don't know how to resize in the case where the L1 table needs new clusters:"))
    else update_header t { t.h with Header.l1_size = Int64.to_int32 l2_tables_required }

  let create base size =
    let version = `Three in
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
    let l2_tables_required = Header.l2_tables_required ~cluster_bits size in
    let nb_snapshots = 0l in
    let snapshots_offset = 0L in
    let additional = Some {
      Header.dirty = true;
      corrupt = false;
      lazy_refcounts = true;
      autoclear_features = 0L;
      refcount_order = 4l;
      } in
    let extensions = [] in
    let h = {
      Header.version; backing_file_offset; backing_file_size;
      cluster_bits = Int32.of_int cluster_bits; size; crypt_method;
      l1_size = Int64.to_int32 l2_tables_required;
      l1_table_offset; refcount_table_offset;
      refcount_table_clusters = Int64.to_int32 refcount_table_clusters;
      nb_snapshots; snapshots_offset; additional; extensions;
    } in
    (* Resize the underlying device to contain the header + refcount table
       + l1 table. Future allocations will enlarge the file. *)
    let l1_size_bytes = Int64.mul 8L l2_tables_required in
    let next_free_byte = Int64.round_up (Int64.add l1_table_offset l1_size_bytes) cluster_size in
    let next_free_cluster = Int64.div next_free_byte cluster_size in
    let open Lwt in
    B.get_info base
    >>= fun base_info ->
    (* make will use the file size to figure out where to allocate new clusters
       therefore we must resize the backing device now *)
    resize_base base base_info.B.sector_size (Physical.make next_free_byte)
    >>*= fun () ->
    make base h
    >>*= fun t ->
    update_header t h
    >>*= fun () ->
    (* Write an initial empty refcount table *)
    let cluster = malloc t.h in
    Cstruct.memset cluster 0;
    B.write base Int64.(div refcount_table_offset (of_int t.base_info.B.sector_size)) [ cluster ]
    >>*= fun () ->
    let rec loop limit i =
      if i = limit
      then Lwt.return (`Ok ())
      else
        Cluster.Refcount.incr t i
        >>*= fun () ->
        loop limit (Int64.succ i) in
    (* Increase the refcount of all header clusters i.e. those < next_free_cluster *)
    loop t.next_cluster 0L
    >>*= fun () ->
    (* Write an initial empty L1 table *)
    B.write base Int64.(div l1_table_offset (of_int t.base_info.B.sector_size)) [ cluster ]
    >>*= fun () ->
    B.flush base
    >>*= fun () ->
    Lwt.return (`Ok t)

  let rebuild_refcount_table t =
    (* Disable lazy refcounts so we actually update the real refcounts *)
    let lazy_refcounts = t.lazy_refcounts in
    t.lazy_refcounts <- false;

    (* Zero all clusters allocated in the refcount table *)
    let buf = malloc t.h in
    let cluster, _ = Physical.to_cluster ~cluster_bits:t.cluster_bits (Physical.make t.h.Header.refcount_table_offset) in
    let rec loop i =
      if i >= Int64.of_int32 t.h.Header.refcount_table_clusters
      then Lwt.return (`Ok ())
      else begin
        ClusterCache.read t.cache Int64.(add cluster i)
          (fun buf ->
             let rec loop i =
               if i >= (Cstruct.len buf)
               then Lwt.return (`Ok ())
               else begin
                 let addr = Physical.make (Cstruct.BE.get_uint64 buf i) in
                 ( if Physical.to_bytes addr <> 0L then begin
                       let cluster, _ = Physical.to_cluster ~cluster_bits:t.cluster_bits addr in
                       ClusterCache.update t.cache cluster
                         (fun buf ->
                            Cstruct.memset buf 0;
                            Lwt.return (`Ok ())
                         )
                     end else Lwt.return (`Ok ()) )
                 >>*= fun () ->
                 loop (8 + i)
               end in
             loop 0
          )
        >>*= fun () ->
        loop (Int64.succ i)
      end in
    loop 0L
    >>*= fun () ->
    (* Increment the refcount of the header, existing refcount table clusters
       and L1 table *)
    Cluster.Refcount.incr t 0L
    >>*= fun () ->
    let rec loop i =
      if i >= Int64.of_int32 t.h.Header.refcount_table_clusters
      then Lwt.return (`Ok ())
      else begin
        Cluster.Refcount.incr t (Int64.add cluster i)
        >>*= fun () ->
        loop (Int64.succ i)
      end in
    loop 0L
    >>*= fun () ->
    let l1_table_clusters =
      let refs_per_cluster = 1L <| (t.cluster_bits - 3) in
      Int64.(div (round_up (of_int32 t.h.Header.l1_size) refs_per_cluster) refs_per_cluster) in
    let l1_table_cluster, _ = Physical.to_cluster ~cluster_bits:t.cluster_bits (Physical.make t.h.Header.l1_table_offset) in
    let loop i =
      if i >= l1_table_clusters
      then Lwt.return (`Ok ())
      else begin
        Cluster.Refcount.incr t (Int64.add l1_table_cluster i)
        >>*= fun () ->
        loop (Int64.succ i)
      end in
    loop 0L
    >>*= fun () ->
    (* Fold over the mapped data, incrementing refcounts along the way *)
    let sectors_per_cluster = Int64.(div (1L <| t.cluster_bits) (of_int t.sector_size)) in
    let rec loop sector =
      if sector >= t.info.size_sectors
      then Lwt.return (`Ok ())
      else begin
        seek_mapped t sector
        >>*= fun mapped_sector ->
        if mapped_sector <> sector
        then loop mapped_sector
        else begin
          Cluster.walk_readonly t (Virtual.make ~cluster_bits:t.cluster_bits Int64.(mul (of_int t.info.sector_size) mapped_sector))
          >>*= function
          | None -> assert false
          | Some offset' ->
            let cluster, _ = Physical.to_sector ~sector_size:t.sector_size offset' in
            Cluster.Refcount.incr t cluster
            >>*= fun () ->
            loop (Int64.add mapped_sector sectors_per_cluster)
        end
      end in
    loop 0L
    >>*= fun () ->
    (* Restore the original lazy_refcount setting *)
    t.lazy_refcounts <- lazy_refcounts;
    Lwt.return (`Ok ())

  let header t = t.h

  type t' = t
  type error' = error
  module Debug = struct
    type t = t'
    type error = error'
    let check_no_overlaps t =
      let l1_table_offset = Physical.make t.h.Header.l1_table_offset in
      let l1_table_cluster, within = Physical.to_cluster ~cluster_bits:t.cluster_bits l1_table_offset in
      assert (within = 0);
      let refcount_table_offset = Physical.make t.h.Header.refcount_table_offset in
      let refcount_table_cluster, within = Physical.to_cluster ~cluster_bits:t.cluster_bits refcount_table_offset in
      assert (within = 0);
      (* This will `assert` if any cluster has a refcount >1 *)
      rebuild_refcount_table t
      >>*= fun () ->
      Lwt.return (`Ok ())

    let set_next_cluster t x = t.next_cluster <- x
  end
end
