
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


let get_nodes_ips state =
    MVar.read state >>= fun self ->
    (* get all nodes uuid *)
    let%lwt nodes = Yaks_connector.Global.Actual.get_all_nodes Yaks_connector.default_system_id Yaks_connector.default_tenant_id self.yaks in
    (* removing self uuid *)
    let nodes = List.filter (fun nid -> (String.compare nid (Apero.Option.get self.configuration.agent.uuid))!=0 ) nodes in
    (* get nodes information *)
    let%lwt nodes_info = Lwt_list.map_p (fun nid ->  Yaks_connector.Global.Actual.get_node_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid self.yaks >>= fun ni ->  Lwt.return (Apero.Option.get ni) ) nodes in
    (* remove the local interface of the remote node, like loopback and virtual interfaces *)
    let open FTypes in
    let%lwt faces_info = Lwt_list.map_p (fun ni ->  Lwt_list.filter_p (fun fi -> Lwt.return (filter_face fi.intf_name) ) ni.network >>= fun fi -> Lwt.return (ni.uuid,fi) ) nodes_info in
    (* extract address information *)
    let ip_infos = Lwt_list.map_p (fun (ni,fi) -> Lwt_list.map_p (fun fi -> Lwt.return (fi.intf_configuration.ipv4_address,fi.intf_configuration.ipv6_address)) fi >>= fun ifl -> Lwt.return (ni, ifl)) faces_info in
    ip_infos



let start_ping_task myuuid connector ni ip port state =
    Logs.debug (fun m -> m "[start_ping_task] - Stating Ping task for %s - Remote IP: %s Local Port: %d" ni ip port);
    let addr = Lwt_unix.ADDR_INET ((Unix.inet_addr_of_string "0.0.0.0"  ), port) in
    let c = Heartbeat.run_client addr state ip in
    let%lwt _ = Lwt_unix.sleep 5.0 in
    let rec statistics_loop state =
            MVar.read state >>= fun(self : Heartbeat.statistics) ->

                let pl = (1.0-.(float(self.packet_received)  /. float(self.packet_sent)))*.100.0 in
                (* Logs.debug (fun m -> m "Average %f Packet loss: %f%% for %s \n" self.avg pl ni); *)
                let pi = FTypes.{peer=ni; ip=ip; average=self.avg; packet_loss = pl; packet_sent=self.packet_sent; packet_received=self.packet_received; bytes_sent=self.bytes_sent; bytes_received=self.bytes_received} in
                let%lwt _ = Yaks_connector.Global.Actual.add_node_neighbor Yaks_connector.default_system_id Yaks_connector.default_tenant_id myuuid ni pi connector in
                Lwt.return self.run

            >>= fun continue ->
            match continue with
            | true ->
              let%lwt _ = Lwt_unix.sleep 5.0 in
              statistics_loop state
            | false -> Lwt.return_unit
      in
      let l = statistics_loop state in
      Lwt.join [c;l]


let prepare_ping_tasks state =
    let%lwt nodes = get_nodes_ips state in
    MVar.guarded state @@ fun self ->
    let%lwt hb_info =  Lwt_list.map_p
          (fun (ni, ipl) ->  Lwt_list.map_p
            (fun (ip,_ )->
              let blen = 1024 in
              let rbuf = Bytes.create blen in
              let state = MVar.create Heartbeat.{peer=ni; timeout=2.0; peer_address=ip; rbuf=rbuf; blen=blen; avg=0.0; packet_sent=0; packet_received=0; bytes_sent=0; bytes_received=0; run=true} in
              Lwt.return state
            ) ipl
          >>= fun hb -> Lwt.return (ni, hb)) nodes
    in
    let rec add_task_inner tasks ni current_tasks =
      match tasks with
      | hd::tl ->
              let new_tasks = List.append current_tasks [hd] in
              add_task_inner tl ni new_tasks
      | [] -> current_tasks
    in
    let rec add_tasks_outer info self =
      match info with
      | hd::tl ->
          let ni,hb = hd in
            let current_tasks =
              match Heartbeat.HeartbeatMap.mem ni self.ping_tasks with
              | false -> []
              | true -> Heartbeat.HeartbeatMap.find ni self.ping_tasks
            in
            let new_tasks = add_task_inner hb ni current_tasks in
            let new_map = Heartbeat.HeartbeatMap.add ni new_tasks self.ping_tasks in
            add_tasks_outer tl {self with ping_tasks = new_map }
      | [] -> self
    in
    let self = add_tasks_outer hb_info self in
    MVar.return () self