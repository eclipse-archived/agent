(*********************************************************************************
 * Copyright (c) 2020 ADLINK Technology Inc.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache Software License 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 * Contributors: 1
 *   Gabriele Baldoni (gabriele (dot) baldoni (at) adlinktech (dot) com ) - Node heartbeat
 *********************************************************************************)


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


type ping_statistics = {
    peer: string;
    timeout: float;
    peer_address : string;
    iface :  string;
    avg : float;
    packet_sent: int;
    packet_received: int;
    bytes_sent: int;
    bytes_received: int;
    peer_sockaddr : Lwt_unix.sockaddr;
    inflight_msgs : (float*int) list;
    last_sq_num : int;
}



type ping_tasks = (string * ping_statistics list)

(* type status =  ping_tasks list *)

type ping_status = {
    sock : Lwt_unix.file_descr;
    rbuf : Bytes.t;
    blen : int;
    run : bool Lwt.t;
    tasks : ping_tasks list;
    unreachable : string list;
}


(* Logs.debug (fun m -> m "[heartbeat_single_threaded.send_ping] - Timeout too big removing %s %s" stat.peer stat.iface);
                    let state = {state with tasks = List.append state.tasks [(peer_id,peer_tasks)]} in
                    Lwt.return state *)


let heartbeat_single_threaded (state : ping_status) =
    Logs.debug (fun m -> m "[heartbeat_single_threaded] - Starting single threaed ping client");
    let rec remove_unreachable tasks newtask =
        match tasks with
        | [] -> newtask
        | hd::tl ->
          let nid,stat = hd in
          let stat = List.filter (fun st -> st.timeout < 10.0) stat in
          remove_unreachable tl (List.append newtask [(nid,stat)])
    in
    let send_ping sock (ping_info : ping_statistics) =
        Logs.debug (fun m -> m "[heartbeat_single_threaded.send_ping] - Sending for %s" ping_info.peer_address);
        let sq_num = ping_info.last_sq_num+1 in
        let msg = HeartbeatMessage.make_ping (Int32.of_int (sq_num)) in
        let buf = HeartbeatMessage.to_buf msg in
        let sbuf = Abuf.read_bytes (Abuf.w_pos buf) buf in
        let t0 = Unix.gettimeofday () in
        let%lwt sent = Lwt_unix.sendto sock sbuf 0 (Bytes.length sbuf) [] ping_info.peer_sockaddr in
        let new_inflight = List.append ping_info.inflight_msgs [t0,sq_num] in
        Lwt.return {ping_info with  inflight_msgs = new_inflight; last_sq_num = sq_num; bytes_sent = ping_info.bytes_sent + sent; packet_sent = ping_info.packet_sent + 1}
    in
    let recv (state : ping_status) =
        Logs.debug (fun m -> m "[heartbeat_single_threaded.recv] - Waiting reply with timeout on recv");
        (* recv with timeout result is (bool * int * float * string)   *)
        let timeout = Lwt_unix.sleep 2.0 >>= fun  _ -> let t1 = Unix.gettimeofday () in Lwt.return (false,0, t1, "") in
        let recv = Lwt_unix.recvfrom state.sock state.rbuf 0 state.blen []
            >>= fun (l, recv_addr) ->
            Logs.debug (fun m -> m "[heartbeat_single_threaded.recv] - Received %d bytes" l);
            let t1 = Unix.gettimeofday () in
            let rcv_ip = match recv_addr with
                    | ADDR_INET (inet,_) -> Unix.string_of_inet_addr inet
                    | _ -> ""
            in
            Lwt.return (true, l, t1, rcv_ip)
        in
        let%lwt res,l,t1,rcv_ip = Lwt.pick [timeout;recv] in
        match res with
        | false ->
            Logs.debug (fun m -> m "[heartbeat_single_threaded.recv] - Timeout");
            let tasks = state.tasks in
            let tasks = List.map (fun (nid,stat) ->
                let stat = List.map (fun (st : ping_statistics ) ->
                    Logs.debug (fun m -> m "[heartbeat_single_threaded.recv] - Updating timeouts for %s" nid);
                    let st = {st with inflight_msgs = List.filter (fun (t,_) -> let rtt = t1-.t in rtt > st.timeout) st.inflight_msgs} in
                    match List.length st.inflight_msgs with
                    | 0 ->  {st with timeout = (if st.timeout < 10.0 then 4.0*.st.timeout else 10.0)}
                    | _ ->
                        let (t0,_) = List.nth st.inflight_msgs ((List.length st.inflight_msgs)-1) in
                        let rtt = (t1-.t0) in
                        Logs.debug (fun m -> m "[heartbeat_single_threaded.recv] - Current RTT for %s is %f - Timeout %f" nid rtt st.timeout);
                        if rtt>st.timeout then {st with timeout = (if st.timeout < 10.0 then 4.0*.st.timeout else 10.0)} else st
                    ) stat in
                Logs.debug (fun m -> m "[heartbeat_single_threaded.recv] - Update done for %s"  nid);
                (nid,stat)
             ) tasks in
            Logs.debug (fun m -> m "[heartbeat_single_threaded.recv] - Returning" );
            Lwt.return {state with tasks = tasks}
        | true ->
        let rabuf = Abuf.from_bytes state.rbuf in
        let rmsg = HeartbeatMessage.from_buf rabuf in
        match rmsg.msg_type with
        | PONG ->
            let sq_num = Int32.to_int rmsg.sequence_number in
            let peer_id = Bytes.to_string rmsg.node_id in
            (* find peer tasks *)
            let _,peer_tasks = List.find (fun (nid,_) -> String.compare nid peer_id == 0) state.tasks in

            (* removing peer tasks it will be readded after the check *)
            let state = {state with tasks = List.filter (fun (n,_) -> (String.compare peer_id n != 0)) state.tasks} in

            (* get task associated with this ip *)
            let stat = List.find (fun stat -> String.compare stat.peer_address rcv_ip == 0 ) peer_tasks in

            (* removing task will be added after check *)
            let peer_tasks = List.filter (fun stat -> String.compare stat.peer_address rcv_ip != 0 ) peer_tasks in


            (* checking *)
            let t0 = List.find (fun (_,sq) -> sq == sq_num) stat.inflight_msgs |> fun (t,_) -> t in
            let rtt = (t1-.t0) in
            if (rtt > stat.timeout) then
                (* dropping if received after timeout *)
                let new_inflight = List.remove_assoc t0 stat.inflight_msgs in
                let new_timeout = if stat.timeout <= 10.0 then 4.0*.stat.timeout else 10.0 in
                let stat = {stat with inflight_msgs = new_inflight; timeout=new_timeout} in
                (* adding single task *)
                let peer_tasks = List.append peer_tasks [stat] in
                (* adding new tasks *)
                let state = {state with tasks = List.append state.tasks [(peer_id,peer_tasks)]} in
                Lwt.return state
            else
                let avg = (stat.avg *. float(stat.packet_received) +. rtt) /. float(stat.packet_received+1) in
                let new_inflight = List.remove_assoc t0 stat.inflight_msgs in
                let stat = {stat with timeout = 3.5*.avg; avg = avg; packet_received = stat.packet_received + 1; bytes_received = stat.bytes_received + l; inflight_msgs = new_inflight} in
                (* adding single task *)
                let peer_tasks = List.append peer_tasks [stat] in
                 (* adding new tasks *)
                let state = {state with tasks = List.append state.tasks [(peer_id,peer_tasks)]} in
                Lwt.return state


        | PING -> Lwt.return state
    in
    let single_run state =
        let peer_ping tasks =
            Lwt_list.map_p (fun stat -> send_ping state.sock stat) tasks
        in
        let all_ping tasks =
            Lwt_list.map_p (fun (p,stat) ->let%lwt pp =  peer_ping stat in Lwt.return (p,pp) ) tasks
        in
        let%lwt new_tasks = all_ping state.tasks  in
        let state = {state with tasks = new_tasks} in
        let%lwt state = recv state in
        let state = {state with tasks = remove_unreachable state.tasks []} in
        Lwt.return state
    in single_run state

let run_server addr nodeid =
    let server_sock = Lwt_unix.socket Lwt_unix.PF_INET Lwt_unix.SOCK_DGRAM 0 in
    Logs.debug (fun m -> m "[run_server] Eclipse fog05 Ping Server -- %s\n" nodeid);
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