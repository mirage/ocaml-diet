type t =
  | Bytes of int64            (** octets from beginning of file *)
  | PhysicalSectors of int64  (** physical sectors on the underlying disk *)
  | Clusters of int64         (** virtual clusters in the qcow image *)
