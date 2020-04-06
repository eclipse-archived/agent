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
 *   Gabriele Baldoni (gabriele (dot) baldoni (at) adlinktech (dot) com ) - 0.2.0 Development iteration
 *********************************************************************************)


open Lwt.Infix
open Fos_sdk
open Agent_state
(* open Fos_sdk.FTypes *)
(* UTILS *)


let filter_face face_name =
    let en_re = Str.regexp "en*" in
    let eth_re = Str.regexp "eth*" in
    let wlan_re = Str.regexp "wlan*" in
    let wl_re = Str.regexp "wl*" in
    let pp_re = Str.regexp "pp*" in
    let ptp_re = Str.regexp "ptp*" in
    let tun_re = Str.regexp "tu*" in
    Str.string_match en_re face_name 0 ||  Str.string_match eth_re face_name 0 || Str.string_match wlan_re face_name 0 || Str.string_match wl_re face_name 0 || Str.string_match pp_re face_name 0 || Str.string_match ptp_re face_name 0 || Str.string_match tun_re face_name 0


let get_network_plugin self =
  MVar.read self >>= fun self ->
  let%lwt plugins = Yaks_connector.Local.Actual.get_node_plugins (Apero.Option.get self.configuration.agent.uuid) self.yaks in
  let%lwt p = Lwt_list.find_s (fun e ->
      let%lwt pl = Yaks_connector.Local.Actual.get_node_plugin (Apero.Option.get self.configuration.agent.uuid) e self.yaks in
      match String.lowercase_ascii pl.plugin_type with
      | "network" -> Lwt.return_true
      | _ -> Lwt.return_false
    ) plugins
  in
  Lwt.return p


let get_nodes myuuid connector =
  let%lwt nodes = Yaks_connector.Global.Actual.get_all_nodes Yaks_connector.default_system_id Yaks_connector.default_tenant_id connector in
  (* removing self uuid *)
  Lwt.return @@ List.filter (fun nid -> (String.compare nid myuuid)!=0 ) nodes

let get_nodes_ips_st myuuid connector =
    (* get all nodes uuid *)
    let%lwt nodes = Yaks_connector.Global.Actual.get_all_nodes Yaks_connector.default_system_id Yaks_connector.default_tenant_id connector in
    (* removing self uuid *)
    let nodes = List.filter (fun nid -> (String.compare nid myuuid)!=0 ) nodes in
    (* get nodes information *)
    let%lwt nodes_info = Lwt_list.map_p (fun nid ->  Yaks_connector.Global.Actual.get_node_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid connector>>= fun ni ->  Lwt.return (Apero.Option.get ni) ) nodes in
    (* remove the local interface of the remote node, like loopback and virtual interfaces *)
    let open FTypes in
    let%lwt faces_info = Lwt_list.map_p (fun ni ->  Lwt_list.filter_p (fun fi -> Lwt.return (filter_face fi.intf_name) ) ni.network >>= fun fi -> Lwt.return (ni.uuid,fi) ) nodes_info in
    (* extract address information *)
    let ip_infos = Lwt_list.map_p (fun (ni,fi) -> Lwt_list.map_p (fun fi -> Lwt.return (fi.intf_name, fi.intf_configuration.ipv4_address,fi.intf_configuration.ipv6_address)) fi >>= fun ifl -> Lwt.return (ni, ifl)) faces_info in
    ip_infos


let add_ping_peer (state : Heartbeat.ping_status) peerid ip_list =
  let tasks = List.map (fun (face,v4,_) ->
    let peer_addr = Lwt_unix.ADDR_INET ((Unix.inet_addr_of_string v4), 9091) in
    let task = Heartbeat.{peer=peerid; iface=face; timeout=2.0; peer_address=v4; avg=0.0; packet_sent=0; packet_received=0; bytes_sent=0; bytes_received=0; inflight_msgs=[]; last_sq_num=(-1); peer_sockaddr=peer_addr} in
    task
    ) ip_list
  in
  let state = {state with tasks = List.append state.tasks [(peerid,tasks)]} in
  state

let start_ping_single_threaded myuuid connector =
  let p,c = Lwt.wait () in
  let port = 9100 + (Random.int 65535) in
  Logs.debug (fun m -> m "[start_ping_single_threaded] - Stating Ping - Local Port: %d" port);
  let addr = Lwt_unix.ADDR_INET ((Unix.inet_addr_of_string "0.0.0.0"  ), port) in
  let sock = Lwt_unix.socket Lwt_unix.PF_INET Lwt_unix.SOCK_DGRAM 0 in
   let%lwt _ = Lwt_unix.bind sock addr in
  let blen = 41 in
  let rbuf = Bytes.create blen in
  let state = Heartbeat.{sock = sock; rbuf=rbuf; blen=blen; run = p; tasks = []; unreachable = []} in
  let rec run state =
    Logs.debug (fun m -> m "[start_ping_single_threaded] Begin run");
    let%lwt nodes = get_nodes_ips_st myuuid connector in
    Logs.debug (fun m -> m "[start_ping_single_threaded] Nodes fetched");
    let rec add_nodes nodes (state:Heartbeat.ping_status) =
      match nodes with
      | hd::tl ->
        let peerid, ip_list = hd in
        let state =
          match List.find_opt (fun (nid,_) -> (String.compare nid peerid == 0)) state.tasks with
          | None ->
            (match List.mem peerid state.unreachable with
            | false ->
              add_ping_peer state peerid ip_list
            | true -> state)
          | Some _ -> state
        in
        add_nodes tl state
      | [] -> state
    in
    let rec remove_nodes nodes (tasks : Heartbeat.ping_tasks list) (newtasks : Heartbeat.ping_tasks list) =
      match tasks with
      | hd::tl ->
        (
          let peerid,stat = hd in
          match List.find_opt (fun (nid,_) -> (String.compare peerid nid ==0 )) nodes with
          | Some _ -> remove_nodes nodes tl  (List.append newtasks [hd])
          | None ->
            Lwt_list.iter_p (fun (e:Heartbeat.ping_statistics) -> Yaks_connector.Global.Actual.remove_node_neighbor Yaks_connector.default_system_id Yaks_connector.default_tenant_id myuuid peerid e.iface connector) stat
            >>= fun _ ->
            remove_nodes nodes tl newtasks)
      | [] -> Lwt.return newtasks
    in
    let rec remove_unrechable (tasks : Heartbeat.ping_tasks list) nodes =
      match nodes with
      | [] -> Lwt.return tasks
      | hd::tl ->
        let ni,stat = hd in
        Lwt_list.iter_p (fun (e:Heartbeat.ping_statistics) -> Yaks_connector.Global.Actual.remove_node_neighbor Yaks_connector.default_system_id Yaks_connector.default_tenant_id myuuid ni e.iface connector) stat
        >>= fun _ ->
        let tasks = List.filter (fun (n,_) -> (String.compare n ni != 0)) tasks in
        remove_unrechable tasks tl
    in
    let state = add_nodes nodes state in
    Logs.debug (fun m -> m "[start_ping_single_threaded] Nodes added");
    let%lwt tasks = (remove_nodes nodes state.tasks []) in
    let state = {state with tasks = tasks } in
    Logs.debug (fun m -> m "[start_ping_single_threaded] Nodes removed");
    let unreachable_nodes = List.filter (fun (_,st) -> match st with | [] -> true | _ -> false ) state.tasks in
    let%lwt tasks = (remove_unrechable state.tasks unreachable_nodes) in
    let state = {state with tasks = tasks } in
    let state = {state with unreachable = List.map (fun (nid,_) -> nid) unreachable_nodes} in
    Logs.debug (fun m -> m "[start_ping_single_threaded] unrechable nodes removed");
    let%lwt state = Heartbeat.heartbeat_single_threaded state in
    Logs.debug (fun m -> m "[start_ping_single_threaded] Single send/receive run done");
    let%lwt _ = Lwt_list.iter_p (
        fun (nid,tasks) ->
          Lwt_list.iter_p
            (fun( stat : Heartbeat.ping_statistics) ->
              let pl = (1.0-.(float(stat.packet_received)  /. float(stat.packet_sent)))*.100.0 in
              Logs.debug (fun m -> m "Average %f Packet loss: %f%% for %s \n" stat.avg pl nid);
              let pi = FTypes.{peer=nid; ip=stat.peer_address; iface=stat.iface; average=stat.avg; packet_loss = pl; packet_sent=stat.packet_sent; packet_received=stat.packet_received; bytes_sent=stat.bytes_sent; bytes_received=stat.bytes_received} in
              Yaks_connector.Global.Actual.add_node_neighbor Yaks_connector.default_system_id Yaks_connector.default_tenant_id myuuid nid stat.iface pi connector
            ) tasks
        ) state.tasks
    in
    Logs.debug (fun m -> m "[start_ping_single_threaded] Published in Zenoh... Now waiting");
    let timeout = Lwt_unix.sleep 5.0 >>= fun  _ -> Lwt.return true in
    let%lwt res = Lwt.choose [timeout;p] in
    Logs.debug (fun m -> m "[start_ping_single_threaded] Result is %b" res);
    match res with
    | false ->
        Logs.debug (fun m -> m "[start_ping_single_threaded] Exiting");
        Lwt.return_unit
    | true ->
      Logs.debug (fun m -> m "[start_ping_single_threaded] Continues");
      run state
  in
  let _ = run state in
  Lwt.return c


let heartbeat_task self =
    Logs.debug (fun m -> m "[heartbeat_task] Eclipse fog05 Heartbeat task start");
    let p,c = Lwt.wait () in
    let rec run self =
      MVar.guarded self (fun state ->
      let myuuid = (Apero.Option.get state.configuration.agent.uuid) in
      let%lwt nodes = get_nodes myuuid state.yaks in
      let%lwt available_nodes = Lwt_list.filter_map_p (fun nid ->
        try%lwt
          let%lwt res = Yaks_connector.Global.Actual.send_heartbeat Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid myuuid state.yaks
            >>= fun r -> Lwt.return (JSON.to_string (Apero.Option.get r.result)) >>=  fun r -> Logs.debug (fun m -> m "[heartbeat_task] r is %s" r); Lwt.return (FTypes.heartbeat_info_of_string r) in
          let timestamp = Unix.gettimeofday () in
          Logs.debug (fun m -> m "[heartbeat_task] Node heartbeat for %s in %f" res.nodeid timestamp);
          Lwt.return @@ Some (nid,timestamp)
        with
        | Fos_errors.FException exn ->
          Logs.err (fun m -> m "[heartbeat_task] Node %s timeout - Exception: %s" nid (Fos_errors.show_ferror exn));
          Lwt.return None
        | exn ->
          Logs.err (fun m -> m "[heartbeat_task] Exception: %s" (Printexc.to_string exn));
          Lwt.return None
      ) nodes
      in
      let state = {state with available_nodes = available_nodes} in
      MVar.return () state)
      >>= fun _ ->
      let timeout = Lwt_unix.sleep 5.0 >>= fun _ -> Lwt.return true in
      let%lwt res = Lwt.choose [timeout;p] in
      Logs.debug (fun m -> m "[heartbeat_task] Continue? %b" res);
      match res with
      | true ->
        Logs.debug (fun m -> m "[heartbeat_task] Continues");
        run self
      | false ->
        Logs.debug (fun m -> m "[heartbeat_task] Exiting");
        Lwt.return_unit
    in
    let _ = run self in
    Lwt.return c
