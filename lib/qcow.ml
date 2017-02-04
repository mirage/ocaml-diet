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
open Result
open Qcow_types
module Error = Qcow_error
module Header = Qcow_header
module Virtual = Qcow_virtual
module Physical = Qcow_physical

module FreeClusters = Qcow_clusterset

let ( <| ) = Int64.shift_left
let ( |> ) = Int64.shift_right_logical

let src =
  let src = Logs.Src.create "qcow" ~doc:"qcow2-formatted BLOCK device" in
  Logs.Src.set_level src (Some Logs.Info);
  src

module Log = (val Logs.src_log src : Logs.LOG)

module Make(Base: Qcow_s.RESIZABLE_BLOCK)(Time: Mirage_time_lwt.S) = struct

  type 'a io = 'a Lwt.t

  (* samoht: `Msg should be the list of all possible exceptions *)
  type error = [ Mirage_block.error | `Msg of string ]

  module Lwt_error = Qcow_error.Lwt_error

  (* samoht: `Msg should be the list of all possible exceptions *)
  type write_error = [ Mirage_block.write_error | `Msg of string ]

  module Lwt_write_error = Qcow_error.Lwt_write_error

  let pp_error ppf = function
    | #Mirage_block.error as e -> Mirage_block.pp_error ppf e
    | `Msg s -> Fmt.string ppf s

  let pp_write_error ppf = function
    | #Mirage_block.write_error as e -> Mirage_block.pp_write_error ppf e
    | `Msg s -> Fmt.string ppf s

  module Config = Qcow_config

  (* Qemu-img will 'allocate' the last cluster by writing only the last sector.
     Cope with this by assuming all later sectors are full of zeroes *)
  module B = Qcow_padded.Make(Base)

  type page_aligned_buffer = B.page_aligned_buffer

  (* Run all threads in parallel, wait for all to complete, then iterate through
     the results and return the first failure we discover. *)
  let iter_p f xs =
    let threads = List.map f xs in
    Lwt_list.fold_left_s (fun acc t ->
        match acc with
        | Error x -> Lwt.return (Error x) (* first error wins *)
        | Ok () -> t
      ) (Ok ()) threads

  module Int64Map = Map.Make(Int64)
  module Int64Set = Set.Make(Int64)


  module Cache = Qcow_cache
  module Recycler = Qcow_recycler.Make(B)
  module Metadata = Qcow_metadata

  module Stats = struct

    type t = {
      mutable nr_erased: int64;
      mutable nr_unmapped: int64;
    }
    let zero = {
      nr_erased = 0L;
      nr_unmapped = 0L;
    }
  end

  module Timer = Qcow_timer.Make(Time)

  type t = {
    mutable h: Header.t;
    base: B.t;
    base_info: Mirage_block.info;
    config: Config.t;
    info: Mirage_block.info;
    cache: Cache.t;
    locks: Qcow_cluster.t;
    recycler: Recycler.t;
    mutable next_cluster: int64; (* when the file is extended *)
    metadata: Metadata.t;
    (* for convenience *)
    cluster_bits: int;
    sector_size: int;
    mutable lazy_refcounts: bool; (* true if we are omitting refcounts right now *)
    mutable stats: Stats.t;
    metadata_lock: Qcow_rwlock.t; (* held to stop the world during compacts and resizes *)
    background_compact_timer: Timer.t;
    mutable cluster_map: Qcow_cluster_map.t; (* a live map of the allocated storage *)
    cluster_map_m: Lwt_mutex.t;
  }

  let get_info t = Lwt.return t.info
  let to_config t = t.config
  let get_stats t = t.stats

  let malloc t =
    let cluster_bits = Int32.to_int t.Header.cluster_bits in
    let npages = max 1 (cluster_bits lsl (cluster_bits - 12)) in
    let pages = Io_page.(to_cstruct (get npages)) in
    Cstruct.sub pages 0 (1 lsl cluster_bits)

  (* Mmarshal a disk physical address written at a given offset within the disk. *)
  let marshal_physical_address t offset v =
    let cluster = Physical.cluster ~cluster_bits:t.cluster_bits offset in
    Metadata.update t.metadata cluster
      (fun c ->
        let addresses = Metadata.Physical.of_cluster c in
        let within = Physical.within_cluster ~cluster_bits:t.cluster_bits offset in
        Metadata.Physical.set addresses within v;
        Lwt.return (Ok ())
      )

  (* Unmarshal a disk physical address written at a given offset within the disk. *)
  let unmarshal_physical_address t offset =
    let cluster = Physical.cluster ~cluster_bits:t.cluster_bits offset in
    Metadata.read t.metadata cluster
      (fun c ->
        let addresses = Metadata.Physical.of_cluster c in
        let within = Physical.within_cluster ~cluster_bits:t.cluster_bits offset in
        Lwt.return (Ok (Metadata.Physical.get addresses within))
      )

  let update_header t h =
    let page = Io_page.(to_cstruct (get 1)) in
    match Header.write h page with
    | Result.Ok _ -> begin
      let open Lwt.Infix in
        B.write t.base 0L [ page ]
        >>= function
        | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
        | Error `Disconnected -> Lwt.return (Error `Disconnected)
        | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
        | Ok () ->
        Recycler.flush t.recycler
        >>= function
        | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
        | Error `Disconnected -> Lwt.return (Error `Disconnected)
        | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
        | Ok () ->
        Log.debug (fun f -> f "Written header");
        t.h <- h;
        Lwt.return (Ok ())
      end
    | Result.Error (`Msg m) ->
      Lwt.return (Error (`Msg m))

  let resize_base base sector_size new_size =
    let sector, within = Physical.to_sector ~sector_size new_size in
    if within <> 0
    then Lwt.return (Error (`Msg (Printf.sprintf "Internal error: attempting to resize to a non-sector multiple %s" (Physical.to_string new_size))))
    else begin
      let open Lwt.Infix in
      B.resize base sector
      >>= function
      | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
      | Error `Disconnected -> Lwt.return (Error `Disconnected)
      | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
      | Ok () ->
      Log.debug (fun f -> f "Resized device to %Ld sectors of size %d" (Qcow_physical.to_bytes new_size) sector_size);
      Lwt.return (Ok ())
    end

  module Cluster = struct

    (** Allocate [n] clusters, registers them as new roots in the cluster map,
        call [f (set, already_zero)] where [set] is a a set of possibly
        non-contiguous physical clusters and [already_zero] indicates whether
        the contents are guaranteed to contain zeroes.

        [f] must cause the clusters to be registered in the cluster map from
        file metadata, otherwise the clusters could be immediately collected.

        This must be called via Qcow_cluster.with_metadata_lock, to prevent
        a parallel thread allocating another cluster for the same purpose.
        *)
    let allocate_clusters t n f =
      if n = 0L then begin
        (* Resync the file size only *)
        let open Lwt_write_error.Infix in
        resize_base t.base t.sector_size (Physical.make (t.next_cluster <| t.cluster_bits))
        >>= fun () ->
        Log.debug (fun f -> f "Resized file to %Ld clusters" t.next_cluster);
        f (FreeClusters.empty, true)
      end else begin
        (* Take them from the free list if they are available *)
        match Recycler.allocate t.recycler n with
        | Some set ->
          Log.debug (fun f -> f "Allocated %Ld clusters from free list" n);
          f (set, true)
        | None ->
          let cluster = t.next_cluster in
          t.next_cluster <- Int64.add t.next_cluster n;
          let free = FreeClusters.Interval.make cluster Int64.(add cluster (pred n)) in
          let set = FreeClusters.(add free empty) in
          Qcow_cluster_map.with_roots t.cluster_map set
            (fun () ->
              Log.debug (fun f -> f "Soft allocated span of clusters from %Ld (length %Ld)" cluster n);
              let open Lwt_write_error.Infix in
              resize_base t.base t.sector_size (Physical.make (t.next_cluster <| t.cluster_bits))
              >>= fun () ->
              f (set, true)
            )
      end

    module Refcount = struct
      (* The refcount table contains pointers to clusters which themselves
         contain the 2-byte refcounts *)

      let zero_all t =
         (* Zero all clusters allocated in the refcount table *)
         let cluster = Physical.cluster ~cluster_bits:t.cluster_bits t.h.Header.refcount_table_offset in
         let rec loop i =
           if i >= Int64.of_int32 t.h.Header.refcount_table_clusters
           then Lwt.return (Ok ())
           else begin
             (* `read` expects the function to be read-only, however we cheat and
                 perform write operations inside the read context *)
             let open Lwt_error.Infix in
             Metadata.read t.metadata Int64.(add cluster i)
               (fun c ->
                 let addresses = Metadata.Physical.of_cluster c in
                 let rec loop i =
                   if i >= Metadata.Physical.len addresses
                   then Lwt.return (Ok ())
                   else begin
                     let open Lwt_write_error.Infix in
                     let addr = Metadata.Physical.get addresses i in
                     ( if Physical.to_bytes addr <> 0L then begin
                            let cluster = Physical.cluster ~cluster_bits:t.cluster_bits addr in
                            Metadata.update t.metadata cluster
                              (fun c ->
                                Metadata.erase c;
                                Lwt.return (Ok ())
                              )
                             >>= fun () ->
                             let open Lwt.Infix in
                             Recycler.flush t.recycler
                             >>= function
                             | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                             | Error `Disconnected -> Lwt.return (Error `Disconnected)
                             | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                             | Ok () -> Lwt.return (Ok ())
                          end else Lwt.return (Ok ())
                      )
                      >>= fun () ->
                      loop (i + 1)
                    end in
                  let open Lwt.Infix in
                  loop 0
                  >>= function
                  | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                  | Error `Disconnected -> Lwt.return (Error `Disconnected)
                  | Error `Is_read_only -> Lwt.return (Error (`Msg "Device is read only"))
                  | Error (`Msg m) -> Lwt.return (Error (`Msg m))
                  | Ok () -> Lwt.return (Ok ())
               )
             >>= fun () ->
             loop (Int64.succ i)
           end in
         loop 0L

      let read t cluster =
        let within_table = Int64.(div cluster (Header.refcounts_per_cluster t.h)) in
        let within_cluster = Int64.(to_int (rem cluster (Header.refcounts_per_cluster t.h))) in

        let offset = Physical.add t.h.Header.refcount_table_offset Int64.(mul 8L within_table) in
        let open Lwt_error.Infix in
        unmarshal_physical_address t offset
        >>= fun offset ->
        if Physical.to_bytes offset = 0L
        then Lwt.return (Ok 0)
        else begin
          let cluster = Physical.cluster ~cluster_bits:t.cluster_bits offset in
          Metadata.read t.metadata cluster
            (fun c ->
              let refcounts = Metadata.Refcounts.of_cluster c in
              Lwt.return (Ok (Metadata.Refcounts.get refcounts within_cluster))
            )
        end

      (** Decrement the refcount of a given cluster. This will never need to allocate.
          We never bother to deallocate refcount clusters which are empty. *)
      let really_decr t cluster =
        let within_table = Int64.(div cluster (Header.refcounts_per_cluster t.h)) in
        let within_cluster = Int64.(to_int (rem cluster (Header.refcounts_per_cluster t.h))) in

        let offset = Physical.add t.h.Header.refcount_table_offset Int64.(mul 8L within_table) in
        let open Lwt_write_error.Infix in
        unmarshal_physical_address t offset
        >>= fun offset ->
        if Physical.to_bytes offset = 0L then begin
          Log.err (fun f -> f "Refcount.decr: cluster %Ld has no refcount cluster allocated" cluster);
          Lwt.return (Error (`Msg (Printf.sprintf "Refcount.decr: cluster %Ld has no refcount cluster allocated" cluster)));
        end else begin
          let cluster = Physical.cluster ~cluster_bits:t.cluster_bits offset in
          Metadata.update t.metadata cluster
            (fun c ->
              let refcounts = Metadata.Refcounts.of_cluster c in
              let current = Metadata.Refcounts.get refcounts within_cluster in
              if current = 0 then begin
                Log.err (fun f -> f "Refcount.decr: cluster %Ld already has a refcount of 0" cluster);
                Lwt.return (Error (`Msg (Printf.sprintf "Refcount.decr: cluster %Ld already has a refcount of 0" cluster)))
              end else begin
                Metadata.Refcounts.set refcounts within_cluster (current - 1);
                Lwt.return (Ok ())
              end
            )
        end

      (** Increment the refcount of a given cluster. Note this might need
          to allocate itself, to enlarge the refcount table. When this function
          returns the refcount is guaranteed to have been persisted. *)
      let rec really_incr t cluster =
        let open Lwt_write_error.Infix in
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
                (fun (free, _already_zero) ->
                  (* Erasing new blocks is handled after the copy *)
                  (* Copy any existing refcounts into new table *)
                  let buf = malloc t.h in
                  let rec loop free i =
                    if i >= Int32.to_int t.h.Header.refcount_table_clusters
                    then Lwt.return (Ok ())
                    else begin
                      let physical = Physical.add t.h.Header.refcount_table_offset Int64.(of_int (i lsl t.cluster_bits)) in
                      let src = Physical.cluster ~cluster_bits:t.cluster_bits physical in
                      let first = FreeClusters.(Interval.x (min_elt free)) in
                      let physical = Physical.make (first <| t.cluster_bits) in
                      let dst = Physical.cluster ~cluster_bits:t.cluster_bits physical in
                      let open Lwt.Infix in
                      Recycler.copy t.recycler src dst
                      >>= function
                      | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                      | Error `Disconnected -> Lwt.return (Error `Disconnected)
                      | Error `Is_read_only -> Lwt.return (Error (`Msg "Device is read only"))
                      | Ok () ->
                      let free = FreeClusters.(remove (Interval.make first first) free) in
                      loop free (i + 1)
                    end in
                  loop free 0
                  >>= fun () ->
                  Log.debug (fun f -> f "Copied refcounts into new table");
                  (* Zero new blocks *)
                  Cstruct.memset buf 0;
                  let rec loop free i =
                    if i >= needed
                    then Lwt.return (Ok ())
                    else begin
                      let first = FreeClusters.(Interval.x (min_elt free)) in
                      let physical = Physical.make (first <| t.cluster_bits) in
                      let sector, _ = Physical.to_sector ~sector_size:t.sector_size physical in
                      let open Lwt.Infix in
                      B.write t.base sector [ buf ]
                      >>= function
                      | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                      | Error `Disconnected -> Lwt.return (Error `Disconnected)
                      | Error `Is_read_only -> Lwt.return (Error (`Msg "Device is read only"))
                      | Ok () ->
                      let free = FreeClusters.(remove (Interval.make first first) free) in
                      loop free (Int64.succ i)
                    end in
                  loop free (Int64.of_int32 t.h.Header.refcount_table_clusters)
                  >>= fun () ->
                  let first = FreeClusters.(Interval.x (min_elt free)) in
                  let refcount_table_offset = Physical.make (first <| t.cluster_bits) in
                  let h' = { t.h with
                             Header.refcount_table_offset;
                             refcount_table_clusters = Int64.to_int32 needed;
                           } in
                  update_header t h'
                  >>= fun () ->
                  (* increase the refcount of the clusters we just allocated *)
                  let rec loop free i =
                    if i >= needed
                    then Lwt.return (Ok ())
                    else begin
                      let first = FreeClusters.(Interval.x (min_elt free)) in
                      really_incr t first
                      >>= fun () ->
                      let free = FreeClusters.(remove (Interval.make first first) free) in
                      loop free (Int64.succ i)
                    end in
                  loop free 0L
                )
            end else begin
            Lwt.return (Ok ())
          end )
        >>= fun () ->

        let offset = Physical.add t.h.Header.refcount_table_offset Int64.(mul 8L within_table) in
        unmarshal_physical_address t offset
        >>= fun addr ->
        ( if Physical.to_bytes addr = 0L then begin
              allocate_clusters t 1L
                (fun (free, _already_zero) ->
                  let cluster = FreeClusters.(Interval.x (min_elt free)) in
                  (* NB: the pointers in the refcount table are different from the pointers
                     in the cluster table: the high order bits are not used to encode extra
                     information and wil confuse qemu/qemu-img. *)
                  let addr = Physical.make (cluster <| t.cluster_bits) in
                  (* zero the cluster *)
                  let buf = malloc t.h in
                  Cstruct.memset buf 0;
                  let sector, _ = Physical.to_sector ~sector_size:t.sector_size addr in
                  let open Lwt.Infix in
                  B.write t.base sector [ buf ]
                  >>= function
                  | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                  | Error `Disconnected -> Lwt.return (Error `Disconnected)
                  | Error `Is_read_only -> Lwt.return (Error (`Msg "Device is read only"))
                  | Ok () ->
                  (* Ensure the new zeroed cluster has been persisted before we reference
                     it via `marshal_physical_address` *)
                  Recycler.flush t.recycler
                  >>= function
                  | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                  | Error `Disconnected -> Lwt.return (Error `Disconnected)
                  | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                  | Ok () ->
                  Log.debug (fun f -> f "Allocated new refcount cluster %Ld" cluster);
                  let open Lwt_write_error.Infix in
                  marshal_physical_address t offset addr
                  >>= fun () ->
                  let open Lwt.Infix in
                  Recycler.flush t.recycler
                  >>= function
                  | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                  | Error `Disconnected -> Lwt.return (Error `Disconnected)
                  | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                  | Ok () ->
                  let open Lwt_write_error.Infix in
                  really_incr t cluster
                  >>= fun () ->
                  Lwt.return (Ok addr)
              )
            end else Lwt.return (Ok addr) )
        >>= fun offset ->
        let refcount_cluster = Physical.cluster ~cluster_bits:t.cluster_bits offset in
        Metadata.update t.metadata refcount_cluster
          (fun c ->
            let refcounts = Metadata.Refcounts.of_cluster c in
            let current = Metadata.Refcounts.get refcounts within_cluster in
            (* We don't support refcounts of more than 1 *)
            assert (current == 0);
            Metadata.Refcounts.set refcounts within_cluster (current + 1);
            Lwt.return (Ok ())
          )
        >>= fun () ->
        let open Lwt.Infix in
        Recycler.flush t.recycler
        >>= function
        | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
        | Error `Disconnected -> Lwt.return (Error `Disconnected)
        | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
        | Ok () ->
        Log.debug (fun f -> f "Incremented refcount of cluster %Ld" cluster);
        Lwt.return (Ok ())

      (* If the lazy refcounts feature is enabled then don't actually Increment
         the refcounts. *)
      let incr t cluster =
        if t.lazy_refcounts
        then Lwt.return (Ok ())
        else really_incr t cluster

      let decr t cluster =
        if t.lazy_refcounts
        then Lwt.return (Ok ())
        else really_decr t cluster

    end


    let read_l1_table t l1_index =
      (* Read l1[l1_index] as a 64-bit offset *)
      let l1_index_offset = Physical.shift t.h.Header.l1_table_offset (Int64.mul 8L l1_index) in
      let open Lwt_error.Infix in
      unmarshal_physical_address t l1_index_offset
      >>= fun l2_table_offset ->
      Lwt.return (Ok l2_table_offset)

    (* Find the first l1_index whose values satisfies [f] *)
    let find_mapped_l1_table t l1_index =
      let open Lwt_error.Infix in
      (* Read l1[l1_index] as a 64-bit offset *)
      let rec loop l1_index =
        if l1_index >= Int64.of_int32 t.h.Header.l1_size
        then Lwt.return (Ok None)
        else begin
          let l1_index_offset = Physical.shift t.h.Header.l1_table_offset (Int64.mul 8L l1_index) in

          let cluster = Physical.cluster ~cluster_bits:t.cluster_bits l1_index_offset in

          Metadata.read t.metadata cluster
            (fun c ->
              let addresses = Metadata.Physical.of_cluster c in
              let within = Physical.within_cluster ~cluster_bits:t.cluster_bits l1_index_offset in
              let rec loop l1_index i : [ `Skip of int | `GotOne of int64 ]=
                if i >= (Metadata.Physical.len addresses) then `Skip i else begin
                  if Metadata.Physical.get addresses i <> Physical.unmapped
                  then `GotOne l1_index
                  else loop (Int64.succ l1_index) (i + 1)
                end in
               Lwt.return (Ok (loop l1_index within)))
          >>= function
          | `GotOne l1_index' ->
            Lwt.return (Ok (Some l1_index'))
          | `Skip n ->
            loop Int64.(add l1_index (of_int n))
        end in
      loop l1_index

    let write_l1_table t l1_index l2_table_offset =
      let open Lwt_write_error.Infix in
      (* Always set the mutable flag *)
      let l2_table_offset =
        if l2_table_offset = Physical.unmapped
        then Physical.unmapped (* don't set metadata bits for unmapped clusters *)
        else Physical.make ~is_mutable:true (Physical.to_bytes l2_table_offset) in
      (* Write l1[l1_index] as a 64-bit offset *)
      let l1_index_offset = Physical.shift t.h.Header.l1_table_offset (Int64.mul 8L l1_index) in
      marshal_physical_address t l1_index_offset l2_table_offset
      >>= fun () ->
      Log.debug (fun f -> f "Written l1_table[%Ld] <- %Ld" l1_index (Physical.cluster ~cluster_bits:t.cluster_bits l2_table_offset));
      Lwt.return (Ok ())

    let read_l2_table t l2_table_offset l2_index =
      let open Lwt_error.Infix in
      let l2_index_offset = Physical.shift l2_table_offset (Int64.mul 8L l2_index) in
      unmarshal_physical_address t l2_index_offset
      >>= fun cluster_offset ->
      Lwt.return (Ok cluster_offset)

    let write_l2_table t l2_table_offset l2_index cluster =
      let open Lwt_write_error.Infix in
      (* Always set the mutable flag *)
      let cluster =
        if cluster = Physical.unmapped
        then Physical.unmapped (* don't set metadata bits for unmapped clusters *)
        else Physical.make ~is_mutable:true (Physical.to_bytes cluster) in
      let l2_index_offset = Physical.shift l2_table_offset (Int64.mul 8L l2_index) in
      marshal_physical_address t l2_index_offset cluster
      >>= fun _ ->
      Log.debug (fun f -> f "Written l2_table[%Ld] <- %Ld" l2_index (Physical.cluster ~cluster_bits:t.cluster_bits cluster));
      Lwt.return (Ok ())

    (* Walk the L1 and L2 tables to translate an address. If a table entry
       is unallocated then return [None]. Note if a [walk_and_allocate] is
       racing with us then we may or may not see the mapping. *)
    let walk_readonly t a =
      let open Lwt_error.Infix in
      Qcow_cluster.with_metadata_lock t.locks
        (fun () ->
          read_l1_table t a.Virtual.l1_index
          >>= fun l2_table_offset ->

          let (>>|=) m f =
            let open Lwt in
            m >>= function
            | Error x -> Lwt.return (Error x)
            | Ok None -> Lwt.return (Ok None)
            | Ok (Some x) -> f x in

          (* Look up an L2 table *)
          ( if Physical.to_bytes l2_table_offset = 0L
            then Lwt.return (Ok None)
            else begin
              if Physical.is_compressed l2_table_offset then failwith "compressed";
              Lwt.return (Ok (Some l2_table_offset))
            end
          ) >>|= fun l2_table_offset ->

          (* Look up a cluster *)
          read_l2_table t l2_table_offset a.Virtual.l2_index
          >>= fun cluster_offset ->
          ( if Physical.to_bytes cluster_offset = 0L
            then Lwt.return (Ok None)
            else begin
              if Physical.is_compressed cluster_offset then failwith "compressed";
              Lwt.return (Ok (Some cluster_offset))
            end
          ) >>|= fun cluster_offset ->

          Lwt.return (Ok (Some (Physical.shift cluster_offset a.Virtual.cluster)))
      )
    (* Walk the L1 and L2 tables to translate an address, allocating missing
       entries as we go. *)
    let walk_and_allocate t a =
      let open Lwt_write_error.Infix in
      Qcow_cluster.with_metadata_lock t.locks
        (fun () ->
           read_l1_table t a.Virtual.l1_index
           >>= fun l2_offset ->
           (* If there is no L2 table entry then allocate L2 and data clusters
              at the same time to minimise I/O *)
           ( if Physical.to_bytes l2_offset = 0L then begin
               allocate_clusters t 2L
                 (fun (free, already_zero) ->
                   (* FIXME: it's unnecessary to write to the data cluster if we're
                      about to overwrite it with real data straight away *)
                   let open Lwt.Infix in
                   ( if not already_zero then Recycler.erase t.recycler free else Lwt.return (Ok ()) )
                   >>= function
                   | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                   | Error `Disconnected -> Lwt.return (Error `Disconnected)
                   | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                   | Ok () ->
                   let open Lwt_write_error.Infix in
                   let l2_cluster = FreeClusters.(Interval.x (min_elt free)) in
                   let free = FreeClusters.(remove (Interval.make l2_cluster l2_cluster) free) in
                   let data_cluster = FreeClusters.(Interval.x (min_elt free)) in
                   Refcount.incr t l2_cluster
                   >>= fun () ->
                   Refcount.incr t data_cluster
                   >>= fun () ->
                   let l2_offset = Physical.make (l2_cluster <| t.cluster_bits) in
                   let data_offset = Physical.make (data_cluster <| t.cluster_bits) in
                   write_l2_table t l2_offset a.Virtual.l2_index data_offset
                   >>= fun () ->
                   write_l1_table t a.Virtual.l1_index l2_offset
                   >>= fun () ->
                   Lwt.return (Ok data_offset)
                )
             end else begin
               read_l2_table t l2_offset a.Virtual.l2_index
               >>= fun data_offset ->
               if Physical.to_bytes data_offset = 0L then begin
                 allocate_clusters t 1L
                   (fun (free, already_zero) ->
                     let open Lwt.Infix in
                     ( if not already_zero then Recycler.erase t.recycler free else Lwt.return (Ok ()) )
                     >>= function
                     | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                     | Error `Disconnected -> Lwt.return (Error `Disconnected)
                     | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                     | Ok () ->
                     let open Lwt_write_error.Infix in
                     let data_cluster = FreeClusters.(Interval.x (min_elt free)) in
                     Refcount.incr t data_cluster
                     >>= fun () ->
                     let data_offset = Physical.make (data_cluster <| t.cluster_bits) in
                     write_l2_table t l2_offset a.Virtual.l2_index data_offset
                     >>= fun () ->
                     Lwt.return (Ok data_offset)
                  )
               end else begin
                 if Physical.is_compressed data_offset then failwith "compressed";
                 Lwt.return (Ok data_offset)
               end
             end
           ) >>= fun data_offset ->
           Lwt.return (Ok (Physical.shift data_offset a.Virtual.cluster))
        )

      let walk_and_deallocate t a =
        let open Lwt_write_error.Infix in
        Qcow_cluster.with_metadata_lock t.locks
          (fun () ->
            read_l1_table t a.Virtual.l1_index
            >>= fun l2_offset ->
            if Physical.to_bytes l2_offset = 0L then begin
              Lwt.return (Ok ())
            end else begin
              read_l2_table t l2_offset a.Virtual.l2_index
              >>= fun data_offset ->
              if Physical.to_bytes data_offset = 0L then begin
                Lwt.return (Ok ())
              end else begin
                (* The data at [data_offset] is about to become an unreferenced
                   hole in the file *)
                let sectors_per_cluster = Int64.(div (1L <| t.cluster_bits) (of_int t.sector_size)) in
                t.stats.Stats.nr_unmapped <- Int64.add t.stats.Stats.nr_unmapped sectors_per_cluster;
                let data_cluster = Physical.cluster ~cluster_bits:t.cluster_bits data_offset in
                write_l2_table t l2_offset a.Virtual.l2_index Physical.unmapped
                >>= fun () ->
                Refcount.decr t data_cluster
              end
            end
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
    Timer.cancel t.background_compact_timer;
    let open Lwt_error.Infix in
    Qcow_rwlock.with_read_lock t.metadata_lock
      (fun () ->
        let cluster_size = 1L <| t.cluster_bits in
        let byte = Int64.(mul sector (of_int t.info.Mirage_block.sector_size)) in
        iter_p (fun (byte, buf) ->
            let vaddr = Virtual.make ~cluster_bits:t.cluster_bits byte in
            Cluster.walk_readonly t vaddr
            >>= function
            | None ->
              Cstruct.memset buf 0;
              Lwt.return (Ok ())
            | Some offset' ->
              (* Qemu-img will 'allocate' the last cluster by writing only the last sector.
                 Cope with this by assuming all later sectors are full of zeroes *)
              let base_sector, _ = Physical.to_sector ~sector_size:t.sector_size offset' in
              let cluster = Physical.cluster ~cluster_bits:t.cluster_bits offset' in
              let open Lwt.Infix in
              Qcow_cluster.with_read_lock t.locks cluster
                (fun () ->
                  B.read t.base base_sector [ buf ]
                )
              >>= function
              | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
              | Error `Disconnected -> Lwt.return (Error `Disconnected)
              | Ok () -> Lwt.return (Ok ())
          ) (chop_into_aligned cluster_size byte bufs)
      )

  let write t sector bufs =
    Timer.cancel t.background_compact_timer;
    let open Lwt_write_error.Infix in
    Qcow_rwlock.with_read_lock t.metadata_lock
      (fun () ->
        let cluster_size = 1L <| t.cluster_bits in
        let byte = Int64.(mul sector (of_int t.info.Mirage_block.sector_size)) in
        iter_p (fun (byte, buf) ->

            let vaddr = Virtual.make ~cluster_bits:t.cluster_bits byte in
            ( Cluster.walk_readonly t vaddr
              >>= function
              | None ->
                (* Only the first write to this area needs to allocate, so it's ok
                   to make this a little slower *)
                Cluster.walk_and_allocate t vaddr
              | Some offset' ->
                Lwt.return (Ok offset') )
            >>= fun offset' ->
            let base_sector, _ = Physical.to_sector ~sector_size:t.sector_size offset' in
            let cluster = Physical.cluster ~cluster_bits:t.cluster_bits offset' in
            let open Lwt.Infix in
            Qcow_cluster.with_write_lock t.locks cluster
              (fun () ->
                B.write t.base base_sector [ buf ]
                >>= function
                | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                | Error `Disconnected -> Lwt.return (Error `Disconnected)
                | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                | Ok () ->
                Log.debug (fun f -> f "Written user data to cluster %Ld" (Physical.cluster ~cluster_bits:t.cluster_bits offset'));
                Lwt.return (Ok ())
              )
          ) (chop_into_aligned cluster_size byte bufs)
      )

  let make_cluster_map t =
    let open Qcow_cluster_map in
    (* Iterate over the all clusters referenced from all the tables in the file
       and (a) construct a set of free clusters; and (b) construct a map of
       physical cluster back to virtual. The free set will show us the holes,
       and the map will tell us where to get the data from to fill the holes in
       with. *)
    let refs = ref ClusterMap.empty in

    let refcount_start_cluster = Physical.cluster ~cluster_bits:t.cluster_bits t.h.Header.refcount_table_offset in
    let int64s_per_cluster = 1L <| (Int32.to_int t.h.Header.cluster_bits - 3) in
    let l1_table_start_cluster = Physical.cluster ~cluster_bits:t.cluster_bits t.h.Header.l1_table_offset in
    let l1_table_clusters = Int64.(div (round_up (of_int32 t.h.Header.l1_size) int64s_per_cluster) int64s_per_cluster) in
    (* Assume all clusters are free. Note when the file is sparse we can exceed the max
       possible cluster. This is only a sanity check to catch crazily-wrong inputs. *)
    let cluster_size = 1L <| t.cluster_bits in
    let max_possible_cluster = (Int64.round_up t.h.Header.size cluster_size) |> t.cluster_bits in
    let free = Qcow_bitmap.make_full
      ~initial_size:(Int64.to_int t.next_cluster)
      ~maximum_size:(Int64.(to_int (mul 10L max_possible_cluster))) in
    (* Subtract the fixed structures at the beginning of the file *)
    Qcow_bitmap.(remove (Interval.make l1_table_start_cluster (Int64.(pred @@ add l1_table_start_cluster l1_table_clusters))) free);
    Qcow_bitmap.(remove (Interval.make refcount_start_cluster (Int64.(pred @@ add refcount_start_cluster (Int64.of_int32 t.h.Header.refcount_table_clusters)))) free);
    Qcow_bitmap.(remove (Interval.make 0L 0L) free);
    let first_movable_cluster =
      try
        Qcow_bitmap.min_elt free
      with
      | Not_found -> t.next_cluster in

    let parse x =
      if x = Physical.unmapped then 0L else begin
        let cluster = Physical.cluster ~cluster_bits:t.cluster_bits x in
        cluster
      end in

    let mark rf cluster =
      let max_cluster = Int64.pred t.next_cluster in
      let c, w = rf in
      if cluster > max_cluster then begin
        Log.err (fun f -> f "Found a reference to cluster %Ld outside the file (max cluster %Ld) from cluster %Ld.%d" cluster max_cluster c w);
        failwith (Printf.sprintf "Found a reference to cluster %Ld outside the file (max cluster %Ld) from cluster %Ld.%d" cluster max_cluster c w);
      end;
      let c, w = rf in
      if cluster = 0L then () else begin
        if ClusterMap.mem cluster !refs then begin
          let c', w' = ClusterMap.find cluster !refs in
          Log.err (fun f -> f "Found two references to cluster %Ld: %Ld.%d and %Ld.%d" cluster c w c' w');
          failwith (Printf.sprintf "Found two references to cluster %Ld: %Ld.%d and %Ld.%d" cluster c w c' w');
        end;
        Qcow_bitmap.(remove (Interval.make cluster cluster) free);
        refs := ClusterMap.add cluster rf !refs;
      end in

    (* scan the refcount table *)
    let open Lwt_error.Infix in
    let rec loop i =
      if i >= Int64.of_int32 t.h.Header.refcount_table_clusters
      then Lwt.return (Ok ())
      else begin
        let refcount_cluster = Int64.(add refcount_start_cluster i) in
        Metadata.read t.metadata refcount_cluster
          (fun c ->
            let addresses = Metadata.Physical.of_cluster c in
            let rec loop i =
              if i >= (Metadata.Physical.len addresses)
              then Lwt.return (Ok ())
              else begin
                let cluster = parse (Metadata.Physical.get addresses i) in
                mark (refcount_cluster, i) cluster;
                loop (i + 1)
              end in
            loop 0
          )
        >>= fun () ->
        loop (Int64.succ i)
      end in
    loop 0L
    >>= fun () ->

    (* scan the L1 and L2 tables, marking the L2 and data clusters *)
    let rec l1_iter i =
      let l1_table_cluster = Int64.(add l1_table_start_cluster i) in
      if i >= l1_table_clusters
      then Lwt.return (Ok ())
      else begin
        Metadata.read t.metadata l1_table_cluster
          (fun c ->
            let l1 = Metadata.Physical.of_cluster c in
            Lwt.return (Ok l1)
          )
        >>= fun l1 ->
        let rec l2_iter i =
          if i >= (Metadata.Physical.len l1)
          then Lwt.return (Ok ())
          else begin
            let l2_table_cluster = parse (Metadata.Physical.get l1 i) in
            if l2_table_cluster <> 0L then begin
              mark (l1_table_cluster, i) l2_table_cluster;
              Metadata.read t.metadata l2_table_cluster
                (fun c ->
                  let l2 = Metadata.Physical.of_cluster c in
                  Lwt.return (Ok l2)
                )
              >>= fun l2 ->
              let rec data_iter i =
                if i >= (Metadata.Physical.len l2)
                then Lwt.return (Ok ())
                else begin
                  let cluster = parse (Metadata.Physical.get l2 i) in
                  mark (l2_table_cluster, i) cluster;
                  data_iter (i + 1)
                end in
              data_iter 0
              >>= fun () ->
              l2_iter (i + 1)
            end else l2_iter (i + 1)
          end in
        l2_iter 0
        >>= fun () ->
        l1_iter (Int64.succ i)
      end in
    l1_iter 0L
    >>= fun () ->

    let map = make ~free ~refs:(!refs) ~first_movable_cluster in

    Lwt.return (Ok map)

  type check_result = {
    free: int64;
    used: int64;
  }

  let check t =
    let open Lwt_error.Infix in
    Qcow_rwlock.with_write_lock t.metadata_lock
      (fun () ->
        let open Qcow_cluster_map in
        make_cluster_map t
        >>= fun block_map ->
        let free = total_free block_map in
        let used = total_used block_map in
        Lwt.return (Ok { free; used })
      )

  type compact_result = {
      copied:       int64;
      refs_updated: int64;
      old_size:     int64;
      new_size:     int64;
  }

  let compact t ?(progress_cb = fun ~percent:_ -> ()) () =
    (* We will return a cancellable task to the caller, and on cancel we will
       set the cancel_requested flag. The main compact loop will detect this
       and complete the moves already in progress before returning. *)
    let cancel_requested = ref false in

    let th, u = Lwt.task () in
    Lwt.on_cancel th (fun () ->
      Log.info (fun f -> f "cancellation of compact requested");
      cancel_requested := true
    );
    (* Catch stray exceptions and return as unknown errors *)
    let open Lwt.Infix in
    Lwt.async
      (fun () ->
        Lwt.catch
          (fun () ->
            let open Lwt_write_error.Infix in
            Qcow_rwlock.with_write_lock t.metadata_lock
              (fun () ->
                let open Qcow_cluster_map in
                let map = t.cluster_map in

                Log.debug (fun f -> f "Physical blocks discovered: %Ld" (total_free map));
                Log.debug (fun f -> f "Total free blocks discovered: %Ld" (total_free map));
                let start_last_block = get_last_block map in

                let sector_size = Int64.of_int t.base_info.Mirage_block.sector_size in
                let cluster_bits = Int32.to_int t.h.Header.cluster_bits in
                let sectors_per_cluster = Int64.div (1L <| cluster_bits) sector_size in

                (* An initial run through only to calculate the total work. We shall
                   treat a block copy and a reference rewrite as a single unit of work
                   even though a block copy is probably bigger. *)
                compact_s (fun _ total_work -> Lwt.return (Ok (true, total_work + 2))) map 0
                >>= fun total_work ->

                (* We shall treat a block copy and a reference rewrite as a single unit of
                   work even though a block copy is probably bigger. *)
                let update_progress =
                  let progress_so_far = ref 0 in
                  let last_percent = ref (-1) in
                  fun () ->
                    incr progress_so_far;
                    let percent = (100 * !progress_so_far) / total_work in
                    if !last_percent <> percent then begin
                      progress_cb ~percent;
                      last_percent := percent
                    end in

                compact_s
                  (fun move () ->
                    let open Lwt.Infix in
                    update_progress ();
                    Recycler.move t.recycler move
                    >>= function
                    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                    | Error `Disconnected -> Lwt.return (Error `Disconnected)
                    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                    | Ok () -> Lwt.return (Ok (not !cancel_requested, ()))
                  ) map ()
                >>= fun () ->

                (* Flush now so that if we crash after updating some of the references, the
                   destination blocks will contain the correct data. *)
                let open Lwt.Infix in
                Recycler.flush t.recycler
                >>= function
                | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                | Error `Disconnected -> Lwt.return (Error `Disconnected)
                | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                | Ok () ->
                let open Lwt_write_error.Infix in

                Recycler.update_references t.recycler
                >>= fun refs_updated ->

                (* Flush now so that the pointers are persisted before we truncate the file *)
                let open Lwt.Infix in
                Recycler.flush t.recycler
                >>= function
                | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
                | Error `Disconnected -> Lwt.return (Error `Disconnected)
                | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
                | Ok () ->

                let last_block = get_last_block map in
                Log.debug (fun f -> f "Shrink file so that last cluster was %Ld, now %Ld" start_last_block last_block);
                t.next_cluster <- Int64.succ last_block;

                let open Lwt_write_error.Infix in
                Cluster.allocate_clusters t 0L (fun _ -> Lwt.return (Ok ())) (* takes care of the file size *)
                >>= fun () ->

                progress_cb ~percent:100;

                let copied = Int64.mul refs_updated sectors_per_cluster in (* one ref per block *)
                let old_size = Int64.mul start_last_block sectors_per_cluster in
                let new_size = Int64.mul last_block sectors_per_cluster in
                let report = { refs_updated; copied; old_size; new_size } in
                Log.info (fun f -> f "%Ld sectors copied, %Ld references updated, file shrunk by %Ld sectors"
                  copied refs_updated (Int64.sub old_size new_size)
                );
                Lwt.return (Ok report)
            )
        ) (fun e ->
          Lwt.return (Error (`Msg (Printexc.to_string e)))
        )
        >>= fun result ->
        Lwt.wakeup u result;
        Lwt.return_unit
      );
    th

  let seek_mapped_already_locked t from =
    let open Lwt_error.Infix in
    let bytes = Int64.(mul from (of_int t.sector_size)) in
    let int64s_per_cluster = 1L <| (Int32.to_int t.h.Header.cluster_bits - 3) in
    let rec scan_l1 a =
      if a.Virtual.l1_index >= Int64.of_int32 t.h.Header.l1_size
      then Lwt.return (Ok Int64.(mul t.info.Mirage_block.size_sectors (of_int t.sector_size)))
      else
        Cluster.find_mapped_l1_table t a.Virtual.l1_index
        >>= function
        | None -> Lwt.return (Ok Int64.(mul t.info.Mirage_block.size_sectors (of_int t.sector_size)))
        | Some l1_index ->
          let a = { a with Virtual.l1_index } in
          Cluster.read_l1_table t a.Virtual.l1_index
          >>= fun x ->
          if Physical.to_bytes x = 0L
          then scan_l1 { a with Virtual.l1_index = Int64.succ a.Virtual.l1_index; l2_index = 0L }
          else
            let rec scan_l2 a =
              if a.Virtual.l2_index >= int64s_per_cluster
              then scan_l1 { a with Virtual.l1_index = Int64.succ a.Virtual.l1_index; l2_index = 0L }
              else
                Cluster.read_l2_table t x a.Virtual.l2_index
                >>= fun x ->
                if Physical.to_bytes x = 0L
                then scan_l2 { a with Virtual.l2_index = Int64.succ a.Virtual.l2_index }
                else Lwt.return (Ok (Qcow_virtual.to_offset ~cluster_bits:t.cluster_bits a)) in
            scan_l2 a in
    scan_l1 (Virtual.make ~cluster_bits:t.cluster_bits bytes)
    >>= fun offset ->
    let x = Int64.(div offset (of_int t.sector_size)) in
    assert (x >= from);
    Lwt.return (Ok x)

  let seek_mapped t from =
    Qcow_rwlock.with_read_lock t.metadata_lock
      (fun () ->
        seek_mapped_already_locked t from
      )

  let seek_unmapped t from =
    let open Lwt_error.Infix in
    Qcow_rwlock.with_read_lock t.metadata_lock
      (fun () ->
        let bytes = Int64.(mul from (of_int t.sector_size)) in
        let int64s_per_cluster = 1L <| (Int32.to_int t.h.Header.cluster_bits - 3) in
        let rec scan_l1 a =
          if a.Virtual.l1_index >= Int64.of_int32 t.h.Header.l1_size
          then Lwt.return (Ok Int64.(mul t.info.Mirage_block.size_sectors (of_int t.sector_size)))
          else
            Cluster.read_l1_table t a.Virtual.l1_index
            >>= fun x ->
            if Physical.to_bytes x = 0L
            then Lwt.return (Ok (Qcow_virtual.to_offset ~cluster_bits:t.cluster_bits a))
            else
              let rec scan_l2 a =
                if a.Virtual.l2_index >= int64s_per_cluster
                then scan_l1 { a with Virtual.l1_index = Int64.succ a.Virtual.l1_index; l2_index = 0L }
                else
                  Cluster.read_l2_table t x a.Virtual.l2_index
                  >>= fun y ->
                  if Physical.to_bytes y = 0L
                  then Lwt.return (Ok (Qcow_virtual.to_offset ~cluster_bits:t.cluster_bits a))
                  else scan_l2 { a with Virtual.l2_index = Int64.succ a.Virtual.l2_index} in
              scan_l2 a in
        scan_l1 (Virtual.make ~cluster_bits:t.cluster_bits bytes)
        >>= fun offset ->
        let x = Int64.(div offset (of_int t.sector_size)) in
        assert (x >= from);
        Lwt.return (Ok x)
      )

  let disconnect t = B.disconnect t.base

  let make config base h =
    let open Lwt in
    B.get_info base
    >>= fun base_info ->
    (* The virtual disk has 512 byte sectors *)
    let info' = {
      Mirage_block.read_write = false;
      sector_size = 512;
      size_sectors = Int64.(div h.Header.size 512L);
    } in
    (* We assume the backing device is resized dynamically so the
       size is the address of the next cluster *)
    let sector_size = base_info.Mirage_block.sector_size in
    let cluster_bits = Int32.to_int h.Header.cluster_bits in
    (* The first cluster is allocated after the L1 table *)
    let size_bytes = Int64.(mul base_info.Mirage_block.size_sectors (of_int sector_size)) in
    let cluster_size = 1L <| cluster_bits in
    (* qemu-img will allocate a cluster by writing only a single sector to the end
       of the file. Therefore we must round up: *)
    let next_cluster = Int64.(div (round_up size_bytes cluster_size) cluster_size) in
    let locks = Qcow_cluster.make () in
    let read_cluster i =
      let buf = malloc h in
      let offset = i <| cluster_bits in
      let sector = Int64.(div offset (of_int sector_size)) in
      let open Lwt.Infix in
      B.read base sector [ buf ]
      >>= function
      | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
      | Error `Disconnected -> Lwt.return (Error `Disconnected)
      | Ok () -> Lwt.return (Ok buf) in
    let write_cluster i buf =
      let offset = i <| cluster_bits in
      let sector = Int64.(div offset (of_int sector_size)) in
      B.write base sector [ buf ]
      >>= function
      | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
      | Error `Disconnected -> Lwt.return (Error `Disconnected)
      | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
      | Ok () -> Lwt.return (Ok ()) in
    let cache = Cache.create ~read_cluster ~write_cluster () in
    let recycler' = ref None in
    let on_unmap x = match !recycler' with
      | None -> ()
      | Some recycler -> Recycler.add_to_junk recycler x in
    let metadata = Metadata.make ~cache ~on_unmap ~cluster_bits ~locks () in
    let recycler = Recycler.create ~base ~sector_size ~cluster_bits ~cache ~locks ~metadata in
    let lazy_refcounts = match h.Header.additional with Some { Header.lazy_refcounts = true; _ } -> true | _ -> false in
    let stats = Stats.zero in
    let metadata_lock = Qcow_rwlock.make () in
    let t = ref None in
    let background_compact_timer = Timer.make ~description:"compact" ~f:(fun () ->
      match !t with
      | None ->
        Lwt.return_unit
      | Some t ->
        (* Don't schedule another compact until the nr_unmapped is above the
           threshold again. *)
        t.stats.Stats.nr_unmapped <- 0L;
        compact t ()
        >>= function
        | Ok _report ->
          Lwt.return_unit
        | Error _e ->
          Log.err (fun f -> f "background compaction returned error");
          Lwt.return_unit
      ) () in
    let cluster_map = Qcow_cluster_map.zero in
    let cluster_map_m = Lwt_mutex.create () in
    let t' = {
      h; base; info = info'; config; base_info;
      locks; recycler; next_cluster;
      metadata; cache; sector_size; cluster_bits; lazy_refcounts; stats; metadata_lock;
      background_compact_timer; cluster_map; cluster_map_m
    } in
    Lwt_error.or_fail_with @@ make_cluster_map t'
    >>= fun cluster_map ->
    t'.cluster_map <- cluster_map;
    Metadata.set_cluster_map t'.metadata cluster_map;
    Recycler.set_cluster_map t'.recycler cluster_map;
    ( if config.Config.discard && not(lazy_refcounts) then begin
        Log.info (fun f -> f "discard requested and lazy_refcounts is disabled: erasing refcount table and enabling lazy_refcounts");
        Lwt_error.or_fail_with @@ Cluster.Refcount.zero_all t'
        >>= fun () ->
        let additional = match h.Header.additional with
          | Some h -> { h with Header.lazy_refcounts = true }
          | None -> {
            Header.dirty = true;
            corrupt = false;
            lazy_refcounts = true;
            autoclear_features = 0L;
            refcount_order = 4l;
            } in
        let extensions = [
          `Feature_name_table Header.Feature.understood
        ] in
        let h = { h with Header.additional = Some additional; extensions } in
        Lwt_write_error.or_fail_with @@ update_header t' h
        >>= fun () ->
        t'.lazy_refcounts <- true;
        Lwt.return_unit
      end else Lwt.return_unit )
    >>= fun () ->
    t := Some t';
    Lwt.return t'

  let connect ?(config=Config.default) base =
    let open Lwt.Infix in
    B.get_info base
    >>= fun base_info ->
    let sector = Cstruct.sub Io_page.(to_cstruct (get 1)) 0 base_info.Mirage_block.sector_size in
    B.read base 0L [ sector ]
    >>= function
    | Error `Unimplemented -> Lwt.fail_with "Unimplemented"
    | Error `Disconnected -> Lwt.fail_with "Disconnected"
    | Ok () ->
      match Header.read sector with
      | Error (`Msg m) -> Lwt.fail_with m
      | Ok (h, _) ->
        make config base h
        >>= fun t ->
        ( if config.Config.check_on_connect then begin
            Lwt_error.or_fail_with @@ check t
            >>= fun { free; used } ->
            Log.info (fun f -> f "image has %Ld free sectors and %Ld used sectors" free used);
            Lwt.return_unit
          end else Lwt.return_unit )
        >>= fun () ->
        Lwt.return t

  let resize t ~new_size:requested_size_bytes ?(ignore_data_loss=false) () =
    Qcow_rwlock.with_write_lock t.metadata_lock
      (fun () ->
        let existing_size = t.h.Header.size in
        if existing_size > requested_size_bytes && not ignore_data_loss
        then Lwt.return (Error(`Msg (Printf.sprintf "Requested resize would result in data loss: requested size = %Ld but current size = %Ld" requested_size_bytes existing_size)))
        else begin
          let size = Int64.round_up requested_size_bytes 512L in
          let l2_tables_required = Header.l2_tables_required ~cluster_bits:t.cluster_bits size in
          (* Keep it simple for now by refusing resizes which would require us to
             reallocate the L1 table. *)
          let l2_entries_per_cluster = 1L <| (Int32.to_int t.h.Header.cluster_bits - 3) in
          let old_max_entries = Int64.round_up (Int64.of_int32 t.h.Header.l1_size) l2_entries_per_cluster in
          let new_max_entries = Int64.round_up l2_tables_required l2_entries_per_cluster in
          if new_max_entries > old_max_entries
          then Lwt.return (Error (`Msg "I don't know how to resize in the case where the L1 table needs new clusters:"))
          else update_header t { t.h with
            Header.l1_size = Int64.to_int32 l2_tables_required;
            size
          }
        end
      )

  let zero =
    let page = Io_page.(to_cstruct (get 1)) in
    Cstruct.memset page 0;
    page

  let rec erase t ~sector ~n () =
    let open Lwt_write_error.Infix in
    if n <= 0L
    then Lwt.return (Ok ())
    else begin
      (* This could walk one cluster at a time instead of one sector at a time *)
      let byte = Int64.(mul sector (of_int t.info.Mirage_block.sector_size)) in
      let vaddr = Virtual.make ~cluster_bits:t.cluster_bits byte in
      ( Cluster.walk_readonly t vaddr
        >>= function
        | None ->
          (* Already zero, nothing to do *)
          Lwt.return (Ok ())
        | Some offset' ->
          let base_sector, _ = Physical.to_sector ~sector_size:t.sector_size offset' in
          t.stats.Stats.nr_erased <- Int64.succ t.stats.Stats.nr_erased;
          let open Lwt.Infix in
          B.write t.base base_sector [ Cstruct.sub zero 0 t.info.Mirage_block.sector_size ]
          >>= function
          | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
          | Error `Disconnected -> Lwt.return (Error `Disconnected)
          | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
          | Ok () -> Lwt.return (Ok ()) )
      >>= fun () ->
      erase t ~sector:(Int64.succ sector) ~n:(Int64.pred n) ()
    end

  let discard t ~sector ~n () =
    let open Lwt_write_error.Infix in
    ( if not(t.config.Config.discard) then begin
        Log.err (fun f -> f "discard called but feature not implemented in configuration");
        Lwt.return (Error `Unimplemented)
      end else Lwt.return (Ok ()) )
    >>= fun () ->
    Timer.cancel t.background_compact_timer;
    Qcow_rwlock.with_read_lock t.metadata_lock
      (fun () ->
        (* we can only discard whole clusters. We will explicitly zero non-cluster
           aligned discards in order to satisfy RZAT *)

        (* round sector, n up to a cluster boundary *)
        let sectors_per_cluster = Int64.(div (1L <| t.cluster_bits) (of_int t.sector_size)) in
        let sector' = Int64.round_up sector sectors_per_cluster in

        (* we can only discard whole clusters. We will explicitly zero non-cluster
           aligned discards in order to satisfy RZAT *)
        let to_erase = min n (Int64.sub sector' sector) in
        erase t ~sector ~n:to_erase ()
        >>= fun () ->

        let n' = Int64.sub n to_erase in

        let rec loop sector n =
          if n < sectors_per_cluster
          then erase t ~sector ~n ()
          else begin
            let byte = Int64.(mul sector (of_int t.info.Mirage_block.sector_size)) in
            let vaddr = Virtual.make ~cluster_bits:t.cluster_bits byte in
            Cluster.walk_and_deallocate t vaddr
            >>= fun () ->
            loop (Int64.add sector sectors_per_cluster) (Int64.sub n sectors_per_cluster)
          end in
        loop sector' n'
      )
    >>= fun () ->
    match t.config.Config.compact_after_unmaps with
    | Some sectors when t.stats.Stats.nr_unmapped > sectors ->
      Timer.restart ~duration_ms:t.config.Config.compact_ms t.background_compact_timer;
      Lwt.return (Ok ())
    | _ -> Lwt.return (Ok ())

  let create base ~size ?(lazy_refcounts=true) ?(config = Config.default) () =
    let version = `Three in
    let backing_file_offset = 0L in
    let backing_file_size = 0l in
    let cluster_bits = 16 in
    let cluster_size = 1L <| cluster_bits in
    let crypt_method = `None in
    (* qemu-img places the refcount table next in the file and only
       qemu-img creates a tiny refcount table and grows it on demand *)
    let refcount_table_offset = Physical.make cluster_size in
    let refcount_table_clusters = 1L in

    (* qemu-img places the L1 table after the refcount table *)
    let l1_table_offset = Physical.make Int64.(mul (add 1L refcount_table_clusters) (1L <| cluster_bits)) in
    let l2_tables_required = Header.l2_tables_required ~cluster_bits size in
    let nb_snapshots = 0l in
    let snapshots_offset = 0L in
    let additional = Some {
      Header.dirty = lazy_refcounts;
      corrupt = false;
      lazy_refcounts;
      autoclear_features = 0L;
      refcount_order = 4l;
      } in
    let extensions = [
      `Feature_name_table Header.Feature.understood
    ] in
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
    let next_free_byte = Int64.round_up (Int64.add (Physical.to_bytes l1_table_offset) l1_size_bytes) cluster_size in
    let open Lwt in
    B.get_info base
    >>= fun base_info ->
    (* make will use the file size to figure out where to allocate new clusters
       therefore we must resize the backing device now *)
    let open Lwt_write_error.Infix in
    resize_base base base_info.Mirage_block.sector_size (Physical.make next_free_byte)
    >>= fun () ->
    let open Lwt.Infix in
    make config base h
    >>= fun t ->
    let open Lwt_write_error.Infix in
    update_header t h
    >>= fun () ->
    (* Write an initial empty refcount table *)
    let cluster = malloc t.h in
    Cstruct.memset cluster 0;
    let open Lwt.Infix in
    B.write base (Physical.sector ~sector_size:t.base_info.Mirage_block.sector_size refcount_table_offset) [ cluster ]
    >>= function
    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
    | Error `Disconnected -> Lwt.return (Error `Disconnected)
    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
    | Ok () ->
    let open Lwt_write_error.Infix in
    let rec loop limit i =
      if i = limit
      then Lwt.return (Ok ())
      else
        Cluster.Refcount.incr t i
        >>= fun () ->
        loop limit (Int64.succ i) in
    (* Increase the refcount of all header clusters i.e. those < next_free_cluster *)
    loop t.next_cluster 0L
    >>= fun () ->
    (* Write an initial empty L1 table *)
    let open Lwt.Infix in
    B.write base (Physical.sector ~sector_size:t.base_info.Mirage_block.sector_size l1_table_offset) [ cluster ]
    >>= function
    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
    | Error `Disconnected -> Lwt.return (Error `Disconnected)
    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
    | Ok () ->
    Recycler.flush t.recycler
    >>= function
    | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
    | Error `Disconnected -> Lwt.return (Error `Disconnected)
    | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
    | Ok () ->
    Lwt.return (Ok t)

  let rebuild_refcount_table t =
    let open Lwt_write_error.Infix in
    Qcow_rwlock.with_write_lock t.metadata_lock
      (fun () ->
        (* Disable lazy refcounts so we actually update the real refcounts *)
        let lazy_refcounts = t.lazy_refcounts in
        t.lazy_refcounts <- false;
        Log.info (fun f -> f "Zeroing existing refcount table");
        Cluster.Refcount.zero_all t
        >>= fun () ->
        let cluster = Physical.cluster ~cluster_bits:t.cluster_bits t.h.Header.refcount_table_offset in
        let rec loop i =
          if i >= Int64.of_int32 t.h.Header.refcount_table_clusters
          then Lwt.return (Ok ())
          else begin
            Cluster.Refcount.incr t (Int64.add cluster i)
            >>= fun () ->
            (* If any of the table entries point to a block, increase its refcount too *)
            Metadata.read t.metadata Int64.(add cluster i)
              (fun c ->
                let addresses = Metadata.Physical.of_cluster c in
                Lwt.return (Ok addresses)
              )
            >>= fun addresses ->
            let rec inner i =
              if i >= (Metadata.Physical.len addresses)
              then Lwt.return (Ok ())
              else begin
                let addr = Metadata.Physical.get addresses i in
                ( if addr <> Physical.unmapped then begin
                    let cluster' = Physical.cluster ~cluster_bits:t.cluster_bits addr in
                    Log.debug (fun f -> f "Refcount cluster %Ld has reference to cluster %Ld" cluster cluster');
                    (* It might have been incremented already by a previous `incr` *)
                    Cluster.Refcount.read t cluster'
                    >>= function
                    | 0 ->
                      Cluster.Refcount.incr t cluster'
                    | _ ->
                      Lwt.return (Ok ())
                  end else Lwt.return (Ok ()) )
                >>= fun () ->
                inner (i + 1)
              end in
            inner 0
            >>= fun () ->
            loop (Int64.succ i)
          end in
        Log.info (fun f -> f "Incrementing refcount of the refcount table clusters");
        loop 0L
        >>= fun () ->
        (* Increment the refcount of the header and L1 table *)
        Log.info (fun f -> f "Incrementing refcount of the header");
        Cluster.Refcount.incr t 0L
        >>= fun () ->
        let l1_table_clusters =
          let refs_per_cluster = 1L <| (t.cluster_bits - 3) in
          Int64.(div (round_up (of_int32 t.h.Header.l1_size) refs_per_cluster) refs_per_cluster) in
        let l1_table_cluster = Physical.cluster ~cluster_bits:t.cluster_bits t.h.Header.l1_table_offset in
        let rec loop i =
          if i >= l1_table_clusters
          then Lwt.return (Ok ())
          else begin
            Cluster.Refcount.incr t (Int64.add l1_table_cluster i)
            >>= fun () ->
            (* Increment clusters of L1 tables *)
            Metadata.read t.metadata Int64.(add l1_table_cluster i)
              (fun c ->
                let addresses = Metadata.Physical.of_cluster c in
                Lwt.return (Ok addresses)
              )
            >>= fun addresses ->
            let rec inner i =
              if i >= (Metadata.Physical.len addresses)
              then Lwt.return (Ok ())
              else begin
                let addr = Metadata.Physical.get addresses i in
                ( if addr <> Physical.unmapped then begin
                    let cluster' = Physical.cluster ~cluster_bits:t.cluster_bits addr in
                    Log.debug (fun f -> f "L1 cluster %Ld has reference to L2 cluster %Ld" cluster cluster');
                    Cluster.Refcount.incr t cluster'
                  end else Lwt.return (Ok ()) )
                >>= fun () ->
                inner (i + 1)
              end in
            inner 0
            >>= fun () ->
            loop (Int64.succ i)
          end in
        Log.info (fun f -> f "Incrementing refcount of the %Ld L1 table clusters starting at %Ld" l1_table_clusters l1_table_cluster);
        loop 0L
        >>= fun () ->
        (* Fold over the mapped data, incrementing refcounts along the way *)
        let sectors_per_cluster = Int64.(div (1L <| t.cluster_bits) (of_int t.sector_size)) in
        let rec loop sector =
          if sector >= t.info.Mirage_block.size_sectors
          then Lwt.return (Ok ())
          else begin
            seek_mapped_already_locked t sector
            >>= fun mapped_sector ->
            if mapped_sector <> sector
            then loop mapped_sector
            else begin
              Cluster.walk_readonly t (Virtual.make ~cluster_bits:t.cluster_bits Int64.(mul (of_int t.info.Mirage_block.sector_size) mapped_sector))
              >>= function
              | None -> assert false
              | Some offset' ->
                let cluster = Physical.cluster ~cluster_bits:t.cluster_bits offset' in
                Cluster.Refcount.incr t cluster
                >>= fun () ->
                loop (Int64.add mapped_sector sectors_per_cluster)
            end
          end in
        Log.info (fun f -> f "Incrementing refcount of the data clusters");
        loop 0L
        >>= fun () ->
        (* Restore the original lazy_refcount setting *)
        t.lazy_refcounts <- lazy_refcounts;
        Lwt.return (Ok ())
    )

  let header t = t.h

  type t' = t
  module Debug = struct
    type t = t'
    type error = write_error
    let check_no_overlaps t =
      let within = Physical.within_cluster ~cluster_bits:t.cluster_bits t.h.Header.l1_table_offset in
      assert (within = 0);
      let within = Physical.within_cluster ~cluster_bits:t.cluster_bits t.h.Header.refcount_table_offset in
      assert (within = 0);
      Lwt.return (Ok ())

    let set_next_cluster t x = t.next_cluster <- x

    let erase_all t =
      let open Lwt.Infix in
      Recycler.erase_all t.recycler
      >>= function
      | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
      | Error `Disconnected -> Lwt.return (Error `Disconnected)
      | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
      | Ok () -> Lwt.return (Ok ())

    let flush t =
      let open Lwt.Infix in
      Recycler.flush t.recycler
      >>= function
      | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
      | Error `Disconnected -> Lwt.return (Error `Disconnected)
      | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
      | Ok () ->
      Log.debug (fun f -> f "Written header");
      Lwt.return (Ok ())

  end
end
