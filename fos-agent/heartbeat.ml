open Lwt.Infix
(* open Yaks_ocaml *)

module MVar = Apero.MVar_lwt

module HeartbeatMap = Map.Make(String)

module HeartbeatMessage = struct

type msg_type =
    | PING
    | PONG

type t = {
    msg_type : msg_type;
    sequence_number : int32;
    node_id : Bytes.t

}

let msg_type_to_int = function
    | PING -> 0x01
    | PONG -> 0x02


let int_of_msg_type = function
    | 0x01 -> PING
    | 0x02 -> PONG
    | _ -> failwith "Cannot match"


let header_size = 5


let make_ping sq =
    {msg_type=PING; sequence_number=sq; node_id=(Bytes.create 0)}


let make_pong sq nodeid =
    let buf = Bytes.create (String.length nodeid) in
    let _ = Bytes.blit_string nodeid 0 buf 0 (String.length nodeid) in
    {msg_type=PONG; sequence_number=sq; node_id=buf}

let to_buf msg =
    let buf = Abuf.create ~grow:32 128 in
    Abuf.write_byte (Char.chr (msg_type_to_int msg.msg_type)) buf;
    Abuf.write_byte (Char.chr(Int32.to_int (Int32.shift_right msg.sequence_number 24))) buf;
    Abuf.write_byte (Char.chr(Int32.to_int (Int32.shift_right (Int32.shift_left msg.sequence_number 8) 24))) buf;
    Abuf.write_byte (Char.chr(Int32.to_int (Int32.shift_right (Int32.shift_left msg.sequence_number 16) 24))) buf;
    Abuf.write_byte (Char.chr(Int32.to_int (Int32.shift_right (Int32.shift_left msg.sequence_number 24) 24))) buf;
    Abuf.write_bytes msg.node_id buf;
    buf


let from_buf buf =
    let msg_type = int_of_msg_type (Char.code (Abuf.read_byte buf)) in
    let b1 = Int32.shift_left (Int32.of_int (Char.code (Abuf.read_byte buf))) 24 in
    let b2 = Int32.shift_left (Int32.of_int (Char.code (Abuf.read_byte buf))) 16 in
    let b3 = Int32.shift_left (Int32.of_int (Char.code (Abuf.read_byte buf))) 8 in
    let b4 = Int32.of_int (Char.code (Abuf.read_byte buf)) in
    let seq = Int32.logor b4 (Int32.logor b3 (Int32.logor b1 b2))
    in
    match msg_type with
    | PING ->  {msg_type; sequence_number=seq; node_id=Bytes.create 0}
    | PONG ->
     let nid = Abuf.read_bytes 36 buf in
     {msg_type; sequence_number=seq; node_id=nid}


end

type statistics = {
    peer: string;
    timeout: float;
    peer_address : string;
    rbuf : Bytes.t;
    blen : int;
    avg : float;
    packet_sent: int;
    packet_received: int;
    bytes_sent: int;
    bytes_received: int;
    run : bool;
}

type t = statistics MVar.t
(*
let self_server_port = 9091
let self_client_port = 9190

let addr_svr = Lwt_unix.ADDR_INET ((Unix.inet_addr_of_string  "127.0.0.1"), self_server_port)
let addr_cli = Lwt_unix.ADDR_INET ((Unix.inet_addr_of_string  "127.0.0.1"), self_client_port)
let yaks_locator = "tcp/127.0.0.1:7447" *)


let run_client addr_cli state address =
    Logs.debug (fun m -> m "[run_client] - Ping client stating for %s" address);

    let sock = Lwt_unix.socket Lwt_unix.PF_INET Lwt_unix.SOCK_DGRAM 0 in
    let server_sock_addr = Lwt_unix.ADDR_INET ((Unix.inet_addr_of_string  address), 9091) in
    let%lwt _ = Lwt_unix.bind sock addr_cli in

    let rec loop sock run state =
        let%lwt result =
            MVar.guarded state @@ fun self ->
            (* check if need to continue ping *)
            match self.run with
            | false -> MVar.return false self
            | true ->
            (* make msg *)
            let msg = HeartbeatMessage.make_ping (Int32.of_int run) in
            let buf = HeartbeatMessage.to_buf msg in
            let sbuf = Abuf.read_bytes (Abuf.w_pos buf) buf in

            (* send and receive *)
            let t0 = Unix.gettimeofday () in
            let%lwt sent = Lwt_unix.sendto sock sbuf 0 (Bytes.length sbuf) [] server_sock_addr in
            let timeout = Lwt_unix.sleep self.timeout >>= fun _ -> Lwt.return (0,false) in
            let ok = Lwt_unix.recvfrom sock self.rbuf 0 self.blen [] >>= fun (l,_) -> Lwt.return (l,true) in
            let%lwt l,res = Lwt.pick [timeout; ok] in
            let t1 = Unix.gettimeofday () in
            let t_tot = (t1-.t0) in
            (* verify result *)
            match res with
            | true ->
                (* decoding *)
                let rabuf = Abuf.from_bytes self.rbuf in
                let rmsg = HeartbeatMessage.from_buf rabuf in
                (match rmsg.msg_type with
                | PONG ->
                    let avg = (self.avg *. float(self.packet_received) +. t_tot) /. float(self.packet_received+1) in
                    let self = {self with timeout = 3.0*.avg; avg = avg; packet_received = self.packet_received + 1; packet_sent = self.packet_sent + 1; bytes_sent = self.bytes_sent + sent; bytes_received = self.bytes_received + l } in
                    MVar.return true self
                | PING ->
                    (* cannot receive a ping in this loop *)
                    failwith "Not expected ping")

            | false ->
                let self = {self with packet_sent = self.packet_sent + 1; bytes_sent = self.bytes_sent + sent; timeout = 2.0*.self.timeout} in
                MVar.return true self
        in
        match result with
        | true ->
            let%lwt _ = Lwt_unix.sleep 1.0 in
            loop sock (run+1) state
        | false -> Lwt.return_unit
    in loop sock 0 state

let run_server addr nodeid =
    let server_sock = Lwt_unix.socket Lwt_unix.PF_INET Lwt_unix.SOCK_DGRAM 0 in
    Logs.debug (fun m -> m "[FOS-AGENT] Eclipse fog05 Ping Server -- %s\n" nodeid);
    let%lwt _ = Lwt_unix.bind server_sock addr in
    let blen = 1024 in
    let buf = Bytes.create blen  in
    let rec serve_client sock addr =
        (* receive, make pong with same sequence number and send *)
        let%lwt l, client = Lwt_unix.recvfrom sock buf 0 blen [] in
        let rmsg = HeartbeatMessage.from_buf (Abuf.from_bytes buf) in
        let smsg = HeartbeatMessage.make_pong rmsg.sequence_number nodeid in
        let sbuf = HeartbeatMessage.to_buf smsg in
        let sbuf = Abuf.read_bytes (Abuf.w_pos sbuf) sbuf in
        let%lwt n = Lwt_unix.sendto sock sbuf 0 (Bytes.length sbuf)  [] client in

        (* ignore variables to avoid error in build *)
        ignore n; ignore l;
        serve_client sock addr
    in
    serve_client server_sock addr




(* let main () =
    let level, reporter = (Apero.Result.get (Logs.level_of_string "debug")) ,  (Logs_fmt.reporter ()) in
    Logs.set_level level;
    Logs.set_reporter reporter;
    try%lwt
        let%lwt yclient = Yaks.login yaks_locator Apero.Properties.empty in
        let%lwt ws = Yaks.workspace (Yaks_types.Path.of_string "/") yclient in
        let blen = 1024 in
        (* let buf = Bytes.create blen in *)
        let rbuf = Bytes.create blen in
        (* let data = "data" in
        let _ = Bytes.blit_string data 0 buf 0 (String.length data) in *)
        let state = {timeout=2.0; server_sock_addr=addr_svr; rbuf=rbuf; blen=blen; avg=0.0; packet_sent=0; packet_received=0; bytes_sent=0; bytes_received=0} in
        let var = MVar.create state in
        let s = run_server addr_svr in
        Logs.debug (fun m -> m "[PingServer] - Server Started");
        let%lwt _ = Lwt_unix.sleep 1.0 in
        let c = run_client addr_cli var in
        Logs.debug (fun m -> m "[PingServer] - Client Started");
        (* ignore s;
        ignore c; *)
        (* ignore c; *)
        let%lwt _ = Lwt_unix.sleep 5.0 in
        let rec statistics_loop state =
            MVar.read state >>= fun self ->
                (* let _ = Lwt_io.printf "Statistics loop\n" in
                let _ =  Lwt_io.flush Lwt_io.stdout in *)
                let pl = (1.0-.(float(self.packet_received)  /. float(self.packet_sent)))*.100.0 in
                Logs.debug (fun m -> m "Average %f Packet loss: %f%% \n" self.avg pl);
                let k = Yaks.Path.of_string "/ping/7B4A683E-350A-42FB-8EB0-5F20C527F86E/" in
                let v = Yaks.Value.StringValue (Printf.sprintf "{\"avg\":%f;\"pl\":%f; \"packet_sent\":%d; \"packet_received\":%d; \"bytes_sent\":%d; \"bytes_received\":%d}" self.avg pl self.packet_sent self.packet_received self.bytes_sent self.bytes_received) in
                let%lwt _ = Yaks.Workspace.put k v ws in
                Lwt.return_unit
            >>= fun _ ->
            let%lwt _ = Lwt_unix.sleep 5.0 in
            statistics_loop state
        in
        let l = statistics_loop var in
        Lwt.join [s;c;l]
    with
  | exn ->
    Logs.debug (fun m -> m "Exception %s raised:\n%s" (Printexc.to_string exn) (Printexc.get_backtrace ()));
    Lwt.return_unit



let () =
    Printexc.record_backtrace true;
    Lwt_main.run @@ main () *)
