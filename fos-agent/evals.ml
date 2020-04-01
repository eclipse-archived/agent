open Lwt.Infix
open Fos_sdk
open Fos_sdk.Errors
open Agent_state
open Utils

(* Evals *)
  let eval_get_fdu_info self (props:Apero.properties) =
    MVar.read self >>= fun state ->
    let fdu_uuid = Apero.Option.get @@ Apero.Properties.get "fdu_uuid" props in
    try%lwt
      let%lwt descriptor = Yaks_connector.Global.Actual.get_catalog_fdu_info  (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id fdu_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let js = FAgentTypes.json_of_string @@ User.Descriptors.FDU.string_of_descriptor descriptor in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_get_image_info self (props:Apero.properties) =
    MVar.read self >>= fun state ->
    let image_uuid = Apero.Option.get @@ Apero.Properties.get "image_uuid" props in
    try%lwt
      let%lwt descriptor = Yaks_connector.Global.Actual.get_image (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id image_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let js = FAgentTypes.json_of_string @@ User.Descriptors.FDU.string_of_image descriptor in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_get_node_fdu_info self (props:Apero.properties) =
    MVar.read self >>= fun state ->
    let fdu_uuid = Apero.Option.get @@ Apero.Properties.get "fdu_uuid" props in
    let node_uuid = Apero.Option.get @@ Apero.Properties.get "node_uuid" props in
    let instanceid = Apero.Option.get @@ Apero.Properties.get "instance_uuid" props in
    try%lwt
      let _ = Logs.debug (fun m -> m "[FOS-AGENT] - eval_get_node_fdu_info - Search for FDU Info") in
      let%lwt descriptor = Yaks_connector.Global.Actual.get_node_fdu_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id node_uuid fdu_uuid instanceid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let js = FAgentTypes.json_of_string @@ Infra.Descriptors.FDU.string_of_record  descriptor in
      let _ = Logs.debug (fun m -> m "[FOS-AGENT] - eval_get_node_fdu_info - INFO %s" (FAgentTypes.string_of_json js)) in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_get_network_info self (props:Apero.properties) =
    MVar.read self >>= fun state ->
    let net_uuid = Apero.Option.get @@ Apero.Properties.get "uuid" props in
    try%lwt
      let%lwt descriptor = Yaks_connector.Global.Actual.get_network (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id net_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let js = FAgentTypes.json_of_string @@ FTypes.string_of_virtual_network descriptor in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error=Some 22; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_get_port_info self (props:Apero.properties) =
    MVar.read self >>= fun state ->
    let cp_uuid = Apero.Option.get @@ Apero.Properties.get "cp_uuid" props in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - eval_get_port_info - Getting info for port %s" cp_uuid ) in
    try%lwt
      let%lwt descriptor = Yaks_connector.Global.Actual.get_port (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id cp_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let js = FAgentTypes.json_of_string @@ User.Descriptors.Network.string_of_connection_point_descriptor  descriptor in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | _ ->
      let _ = Logs.debug (fun m -> m "[FOS-AGENT] - eval_get_port_info - Search port on FDU") in
      let%lwt fdu_ids = Yaks_connector.Global.Actual.get_catalog_all_fdus (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id state.yaks in
      let%lwt cps = Lwt_list.filter_map_p (fun e ->
          let%lwt fdu =  Yaks_connector.Global.Actual.get_catalog_fdu_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id e state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
          let%lwt c = Lwt_list.filter_map_p (fun (cp:User.Descriptors.Network.connection_point_descriptor) ->
              let _ = Logs.debug (fun m -> m "[FOS-AGENT] - eval_get_port_info - %s == %s ? %d " cp.id cp_uuid (String.compare cp.id  cp_uuid)) in
              if (String.compare cp.id cp_uuid) == 0 then  Lwt.return @@ Some cp
              else Lwt.return None
            ) fdu.connection_points
          in Lwt.return @@ List.nth_opt c 0
        ) fdu_ids
      in
      try%lwt
        let cp = List.hd cps in
        let js = FAgentTypes.json_of_string @@ User.Descriptors.Network.string_of_connection_point_descriptor cp in
        let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
      with
      | exn ->
        let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_get_node_mgmt_address self (props:Apero.properties) =
    MVar.read self >>= fun state ->
    let node_uuid = Apero.Option.get @@ Apero.Properties.get "node_uuid" props in
    try%lwt
      let%lwt nconf = Yaks_connector.Global.Actual.get_node_configuration (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id node_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let%lwt descriptor = Yaks_connector.Global.Actual.get_node_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id node_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let nws = descriptor.network in
      let%lwt addr = (Lwt_list.filter_map_p (
          fun (e:FTypes.network_spec_type) ->
            if (String.compare e.intf_name  nconf.agent.mgmt_interface) == 0 then
              Lwt.return @@ Some e.intf_configuration
            else
              Lwt.return None
        ) nws) >>= fun l -> Lwt.return @@ List.hd l
      in
      let js = FAgentTypes.json_of_string @@ FTypes.string_of_intf_conf_type addr in
      let eval_res = FAgentTypes.{result =  Some js; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* NM Evals *)
  let eval_create_net self (props:Apero.properties) =
    MVar.read self >>= fun state ->
    try%lwt
      let%lwt net_p = get_network_plugin self in
      let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CREATE-NET - ##############") in
      let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CREATE-NET - Properties: %s" (Apero.Properties.to_string props) ) in
      let descriptor = FTypes.virtual_network_of_string @@ Apero.Option.get @@ Apero.Properties.get "descriptor" props in
      let record = FTypesRecord.{uuid = descriptor.uuid; status = `CREATE; properties = None; ip_configuration = descriptor.ip_configuration; overlay = None; vni = None; mcast_addr = None; vlan_id = None; face = None} in
      Yaks_connector.Local.Desired.add_node_network (Apero.Option.get state.configuration.agent.uuid) net_p descriptor.uuid record state.yaks
      >>= fun _ ->
      let js = JSON.of_string @@ FTypesRecord.string_of_virtual_network record in
      let eval_res = FAgentTypes.{result = Some js ; error=None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let bt = Printexc.get_backtrace () in
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-CREATE-NET - EXCEPTION: %s TRACE %s " (Printexc.to_string exn) bt) in
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_remove_net self (props:Apero.properties) =
    MVar.read self >>= fun state ->
    try%lwt
      let%lwt net_p = get_network_plugin self in
      let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-REMOVE-NET - ##############") in
      let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-REMOVE-NET - Properties: %s" (Apero.Properties.to_string props) ) in
      let net_id =Apero.Option.get @@ Apero.Properties.get "net_id" props in
      let%lwt record =  Yaks_connector.Local.Actual.get_node_network  (Apero.Option.get state.configuration.agent.uuid) net_p net_id state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let record = {record with status = `DESTROY} in
      let%lwt _ = Yaks_connector.Local.Desired.add_node_network (Apero.Option.get state.configuration.agent.uuid) net_p net_id record state.yaks in
      Yaks_connector.Global.Actual.remove_network (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id record.uuid state.yaks >>= Lwt.return
      >>= fun _ ->
      let js = JSON.of_string @@ FTypesRecord.string_of_virtual_network record in
      let eval_res = FAgentTypes.{result = Some js ; error=None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-REMOVE-NET - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_create_cp self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CREATE-CP - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CREATE-CP - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let descriptor = User.Descriptors.Network.connection_point_descriptor_of_string @@ Apero.Option.get @@ Apero.Properties.get "descriptor" props in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CREATE-CP - # NetManager: %s" net_p) in
    try%lwt
      let parameters = [("descriptor",User.Descriptors.Network.string_of_connection_point_descriptor descriptor)] in
      let fname = "create_port_agent" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
      | Some r -> Lwt.return @@ FAgentTypes.string_of_eval_result r
      | None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot connect create cp %s") descriptor.id ),503)))
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-CREATE-CP - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_remove_cp self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-REMOVE-CP - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-REMOVE-CP - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let cp_id =  Apero.Option.get @@ Apero.Properties.get "cp_id" props in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-REMOVE-CP - # NetManager: %s" net_p) in
    try%lwt
      let parameters = [("cp_id", cp_id)] in
      let fname = "destroy_port_agent" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
      | Some r -> Lwt.return @@ FAgentTypes.string_of_eval_result r
      | None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot destroy create cp %s") cp_id ),503)))
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-REMOVE-CP - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_connect_cp_to_fdu_face self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CONNECT-CP-TO-FDU - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CONNECT-CP-TO-FDU - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let cp_id = Apero.Option.get @@ Apero.Properties.get "cp_id" props in
    let instance_id = Apero.Option.get @@ Apero.Properties.get "instance_id" props in
    let interface = Apero.Option.get @@ Apero.Properties.get "interface" props in
    try%lwt


      (* let%lwt nodeid = Yaks_connector.Global.Actual.get_fdu_instance_node (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id instance_id state.yaks in
         let nodeid =
         match nodeid with
         | Some nid -> nid
         | None ->  raise @@ FException (`InternalError (`Msg ("Unable to find nodeid for this instance id" ) ))
         in *)

      let%lwt record = Yaks_connector.Global.Actual.get_node_fdu_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get state.configuration.agent.uuid) "*" instance_id state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CONNECT-CP-TO-FDU - FDU Record: %s" (Infra.Descriptors.FDU.string_of_record record) ) in
      (* Find Correct Plugin *)
      let fdu_type = Fos_sdk.string_of_hv_type record.hypervisor in
      let%lwt plugins = Yaks_connector.Local.Actual.get_node_plugins (Apero.Option.get state.configuration.agent.uuid) state.yaks in
      let%lwt matching_plugins = Lwt_list.filter_map_p (fun e ->
          let%lwt pl = Yaks_connector.Local.Actual.get_node_plugin (Apero.Option.get state.configuration.agent.uuid) e state.yaks in
          if String.uppercase_ascii (pl.name) = String.uppercase_ascii (fdu_type) then
            Lwt.return @@ Some pl
          else
            Lwt.return None
        ) plugins
      in
      let pl =
        match matching_plugins with
        | [] ->
          ignore @@  Logs.err (fun m -> m "Cannot find a plugin for this FDU even if it is present in the node WTF!! %s" instance_id );
          None
        | _ -> Some  ((List.hd matching_plugins).uuid)
      in
      (* Create Record
       * Add UUID for each component
       * Fix references with UUIDs
      *)
      (match pl with
       | Some plid ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CONNECT-CP-TO-FDU - Plugin ID: %s" plid ) in
         let parameters = [("cpid", cp_id);("instanceid", instance_id);("iface",interface)] in
         let fname = "connect_interface_to_cp" in
         Yaks_connector.Local.Actual.exec_plugin_eval (Apero.Option.get state.configuration.agent.uuid) plid fname parameters state.yaks
         >>= fun res ->
         (match res with
          | Some r ->
            let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CONNECT-CP-TO-FDU - Should be ok!") in
            Lwt.return @@ FAgentTypes.string_of_eval_result r
          | None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot connect cp to interface %s") cp_id ),503)))
         )
       | None ->
         Lwt.fail @@ FException (`PluginNotFound (`MsgCode ((Printf.sprintf ("CRITICAL!!!! Cannot find a plugin for this FDU even if it is present in the node WTF!! %s") instance_id ),404))))
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-CONNECT-CP-TO-FDU - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_disconnect_cp_from_fdu_face self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DISCONNECT-CP-TO-FDU - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DISCONNECT-CP-TO-FDU - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let face = Apero.Option.get @@ Apero.Properties.get "interface" props in
    let instance_id = Apero.Option.get @@ Apero.Properties.get "instance_id" props in
    try%lwt


      let%lwt record = Yaks_connector.Global.Actual.get_node_fdu_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get state.configuration.agent.uuid) "*" instance_id state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      (* Find Correct Plugin *)
      let fdu_type = Fos_sdk.string_of_hv_type record.hypervisor in
      let%lwt plugins = Yaks_connector.Local.Actual.get_node_plugins (Apero.Option.get state.configuration.agent.uuid) state.yaks in
      let%lwt matching_plugins = Lwt_list.filter_map_p (fun e ->
          let%lwt pl = Yaks_connector.Local.Actual.get_node_plugin (Apero.Option.get state.configuration.agent.uuid) e state.yaks in
          if String.uppercase_ascii (pl.name) = String.uppercase_ascii (fdu_type) then
            Lwt.return @@ Some pl
          else
            Lwt.return None
        ) plugins
      in
      let pl =
        match matching_plugins with
        | [] ->
          ignore @@  Logs.err (fun m -> m "Cannot find a plugin for this FDU even if it is present in the node WTF!! %s" instance_id );
          None
        | _ -> Some  ((List.hd matching_plugins).uuid)
      in
      (* Create Record
       * Add UUID for each component
       * Fix references with UUIDs
      *)
      (match pl with
       | Some plid ->
         let parameters = [("iface", face);("instanceid", instance_id)] in
         let fname = "disconnect_interface_from_cp" in
         Yaks_connector.Local.Actual.exec_plugin_eval (Apero.Option.get state.configuration.agent.uuid) plid fname parameters state.yaks
         >>= fun res ->
         (match res with
          | Some r -> Lwt.return @@ FAgentTypes.string_of_eval_result r
          | None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot disconnect cp from interface %s") face ),503)))
         )
       | None ->
         Lwt.fail @@ FException (`PluginNotFound (`MsgCode ((Printf.sprintf ("CRITICAL!!!! Cannot find a plugin for this FDU even if it is present in the node WTF!! %s") instance_id ),404))))
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-DEFINE-FDU - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_connect_cp_to_network self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CONNECT-CP - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CONNECT-CP - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let cp_id = Apero.Option.get @@ Apero.Properties.get "cp_uuid" props in
    let net_id = Apero.Option.get @@ Apero.Properties.get "network_uuid" props in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CONNECT-CP - # NetManager: %s" net_p) in
    try%lwt
      let parameters = [("cp_id",cp_id);("vnet_id", net_id)] in
      let fname = "connect_cp_to_vnetwork" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
      | Some r -> Lwt.return @@ FAgentTypes.string_of_eval_result r
      | None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot connect cp %s to netwokr %s") cp_id net_id ),503)))
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_remove_cp_from_network self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DISCONNECT-CP - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DISCONNECT-CP - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let cp_id = Apero.Option.get @@ Apero.Properties.get "cp_uuid" props in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DISCONNECT-CP - # NetManager: %s" net_p) in
    try%lwt
      let parameters = [("cp_id",cp_id)] in
      let fname = "disconnect_cp" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
      | Some r -> Lwt.return @@ FAgentTypes.string_of_eval_result r
      | None -> Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot Remove cp %s from netwokr") cp_id ),503)))
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* FDU Onboard in Catalog -- this may be moved to Orchestration part *)
  let eval_onboard_fdu self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ONBOARD-FDU - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ONBOARD-FDU - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let descriptor = Apero.Option.get @@ Apero.Properties.get "descriptor" props in
    try%lwt
      let descriptor = User.Descriptors.FDU.descriptor_of_string descriptor in
      let descriptor =
        match descriptor.uuid with
        | Some _ -> descriptor
        | None ->
          let fduid = Apero.Uuid.to_string @@ Apero.Uuid.make_from_alias descriptor.id in
          {descriptor with uuid = Some fduid}
      in
      Yaks_connector.Global.Actual.add_catalog_fdu_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get descriptor.uuid) descriptor state.yaks
      >>= fun _ ->
      let js = JSON.of_string (User.Descriptors.FDU.string_of_descriptor descriptor) in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-ONBOARD-FDU - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* FDU Definition in Node *)
  let eval_define_fdu self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEFINE-FDU - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEFINE-FDU - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let fdu_uuid = Apero.Option.get @@ Apero.Properties.get "fdu_id" props in
    try%lwt
      let%lwt descriptor = Yaks_connector.Global.Actual.get_catalog_fdu_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id fdu_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      (* Find Correct Plugin *)
      let fdu_type = Fos_sdk.string_of_hv_type descriptor.hypervisor in
      let%lwt plugins = Yaks_connector.Local.Actual.get_node_plugins (Apero.Option.get state.configuration.agent.uuid) state.yaks in
      let%lwt matching_plugins = Lwt_list.filter_map_p (fun e ->
          let%lwt pl = Yaks_connector.Local.Actual.get_node_plugin (Apero.Option.get state.configuration.agent.uuid) e state.yaks in
          if String.uppercase_ascii (pl.name) = String.uppercase_ascii (fdu_type) then
            Lwt.return @@ Some pl
          else
            Lwt.return None
        ) plugins
      in
      let pl =
        match matching_plugins with
        | [] -> None
        | _ -> Some  ((List.hd matching_plugins).uuid)
      in
      (* Create Record
       * Add UUID for each component
       * Fix references with UUIDs
      *)
      let instanceid = Apero.Uuid.to_string @@ Apero.Uuid.make () in
      let cp_records = List.map (
          fun (e:User.Descriptors.Network.connection_point_descriptor) ->
            let cpuuid = Apero.Uuid.to_string @@ Apero.Uuid.make () in
            Infra.Descriptors.Network.{  uuid = cpuuid; status = `CREATE; cp_id = e.id;
                                         cp_type = e.cp_type; port_security_enabled = e.port_security_enabled;
                                         properties = None; veth_face_name = None; br_name = None; vld_ref = e.vld_ref
                                      }
        ) descriptor.connection_points
      in
      let interface_records = List.map (fun (e:User.Descriptors.FDU.interface) ->
          let cp_new_id =
            match e.cp_id with
            | Some cp_id ->
              let cp =  List.find (fun (cp:Infra.Descriptors.FDU.connection_point_record) -> cp_id = cp.cp_id ) cp_records
              in Some cp.uuid
            | None -> None
          in
          match e.virtual_interface.intf_type with
          | `PHYSICAL | `BRIDGED ->
            let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEFINE-FDU - THIS FDU HAS PHYSICAL INTERFACE!!!!!!!!!!!!") in
            let  r = Infra.Descriptors.FDU.{name = e.name; is_mgmt = e.is_mgmt; if_type = e.if_type;
                                            mac_address = e.mac_address; virtual_interface = e.virtual_interface;
                                            cp_id = cp_new_id; ext_cp_id = e.ext_cp_id;
                                            vintf_name = e.name; status = `CREATE; phy_face = Some e.virtual_interface.vpci;
                                            veth_face_name = None; properties = None}
            in
            let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEFINE-FDU - THIS FDU HAS PHYSICAL INTERFACE RECORD: %s" (Infra.Descriptors.FDU.string_of_interface r) ) in
            r
          | _ ->
            Infra.Descriptors.FDU.{name = e.name; is_mgmt = e.is_mgmt; if_type = e.if_type;
                                   mac_address = e.mac_address; virtual_interface = e.virtual_interface;
                                   cp_id = cp_new_id; ext_cp_id = e.ext_cp_id;
                                   vintf_name = e.name; status = `CREATE; phy_face = None;
                                   veth_face_name = None; properties = None}


        ) descriptor.interfaces
      in
      let storage_records = List.map (fun (e:User.Descriptors.FDU.storage_descriptor) ->
          let st_uuid = Apero.Uuid.to_string @@ Apero.Uuid.make () in
          let cp_new_id =
            match e.cp_id with
            | Some cp_id ->
              let cp =  List.find (fun (cp:Infra.Descriptors.FDU.connection_point_record) -> cp_id = cp.cp_id ) cp_records
              in Some cp.cp_id
            | None -> None
          in
          Infra.Descriptors.FDU.{uuid = st_uuid; storage_id = e.id;
                                 storage_type = e.storage_type; size = e.size;
                                 file_system_protocol = e.file_system_protocol;
                                 cp_id = cp_new_id}
        ) descriptor.storage
      in
      let record = Infra.Descriptors.FDU.{
          uuid = instanceid;
          fdu_id = Apero.Option.get @@ descriptor.uuid;
          status = `DEFINE;
          image = descriptor.image;
          command = descriptor.command;
          storage = storage_records;
          computation_requirements = descriptor.computation_requirements;
          geographical_requirements = descriptor.geographical_requirements;
          energy_requirements = descriptor.energy_requirements;
          hypervisor = descriptor.hypervisor;
          migration_kind = descriptor.migration_kind;
          configuration = descriptor.configuration;
          interfaces = interface_records;
          io_ports = descriptor.io_ports;
          connection_points = cp_records;
          depends_on = descriptor.depends_on;
          error_code = None;
          error_msg = None;
          migration_properties = None;
          hypervisor_info = JSON.create_empty ()
        }
      in
      (match pl with
       | Some plid ->
         Yaks_connector.Local.Desired.add_node_fdu (Apero.Option.get state.configuration.agent.uuid) plid fdu_uuid instanceid record state.yaks
         >>= fun _ ->
         let js = JSON.of_string (Infra.Descriptors.FDU.string_of_record record) in
         let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
         Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
       | None ->
         Lwt.fail @@ FException (`PluginNotFound (`MsgCode ((Printf.sprintf ("Node %s has no plugin for %s") (Apero.Option.get state.configuration.agent.uuid) fdu_uuid ),404))))
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-DEFINE-FDU - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* FDU Requirements Checks *)
  (* At Intitial implementation just checks cpu architecture, ram and if there is a plugin for the FDU
   * More checks needed:
   * CPU Count
   * CPU Freq
   * Disk space
   * GPUs
   * FPGAs
   * I/O Devices
     And idea can be having some filters functions that return boolean value and run this function one after the other using AND logical operation
     eg.
     let compatible = true in
     let compatible = compatible and run_cpu_filter fdu node_info in
     let compatible = compatible and run_ram_filter fdu node_info in
     let compatible = compatible and run_disk_filter fdu node_info in
     ....


  *)
  let eval_check_fdu self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CHECK-FDU - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CHECK-FDU - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let descriptor = Apero.Option.get @@ Apero.Properties.get "descriptor" props in
    try%lwt
      let descriptor = User.Descriptors.FDU.descriptor_of_string descriptor in
      let%lwt node_info = Yaks_connector.Global.Actual.get_node_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get state.configuration.agent.uuid) state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let comp_requirements = descriptor.computation_requirements in
      let compare (fdu_cp:User.Descriptors.FDU.computational_requirements)  (ninfo:FTypes.node_info) =
        let ncpu = List.hd ninfo.cpu in
        let fdu_type = Fos_sdk.string_of_hv_type descriptor.hypervisor in
        let%lwt plugins = Yaks_connector.Local.Actual.get_node_plugins (Apero.Option.get state.configuration.agent.uuid) state.yaks in
        let%lwt matching_plugins = Lwt_list.filter_map_p (fun e ->
            let%lwt pl = Yaks_connector.Local.Actual.get_node_plugin (Apero.Option.get state.configuration.agent.uuid) e state.yaks in
            if String.uppercase_ascii (pl.name) = String.uppercase_ascii (fdu_type) then
              Lwt.return (Some pl.uuid)
            else
              Lwt.return None
          ) plugins
        in
        let has_plugin =
          match matching_plugins with
          | [] -> false
          | _ -> true
        in
        let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CHECK-FDU - CPU Arch Check: %s = %s ? %b" fdu_cp.cpu_arch ncpu.arch ((String.compare fdu_cp.cpu_arch ncpu.arch) == 0) ) in
        let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CHECK-FDU - RAM Size Check: %b" (fdu_cp.ram_size_mb <= ninfo.ram.size) ) in
        let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-CHECK-FDU - Plugin Check: %b" has_plugin ) in
        match ((String.compare fdu_cp.cpu_arch ncpu.arch) == 0),(fdu_cp.ram_size_mb <= ninfo.ram.size), has_plugin with
        | (true, true, true) -> Lwt.return true
        | (_,_,_) -> Lwt.return false
      in
      let%lwt res = compare comp_requirements node_info in
      let res = match res with
        | true -> FAgentTypes.{uuid = (Apero.Option.get state.configuration.agent.uuid); is_compatible=true }
        | false ->  FAgentTypes.{uuid = (Apero.Option.get state.configuration.agent.uuid); is_compatible=false }
      in
      let js = JSON.of_string (FAgentTypes.string_of_compatible_node_response res) in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-CHECK-FDU - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg =  None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* Entity Onboard in Catalog *)
  let eval_onboard_entity self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ONBOARD-ENTITY - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ONBOARD-ENTITY - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let descriptor = Apero.Option.get @@ Apero.Properties.get "descriptor" props in
    try%lwt
      let descriptor = User.Descriptors.Entity.descriptor_of_string descriptor in
      let descriptor =
        match descriptor.uuid with
        | Some _ -> descriptor
        | None ->
          let fduid = Apero.Uuid.to_string @@ Apero.Uuid.make_from_alias descriptor.id in
          {descriptor with uuid = Some fduid}
      in
      (* Here have to verify that the Atomic Entities composing the entity are already present in the catalog if query where available this will result in a selector with a query, that is faster*)
      let%lwt aes = Yaks_connector.Global.Actual.get_catalog_all_atomic_entities (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id state.yaks  >>=
        Lwt_list.map_p (fun (e:string) ->
            let%lwt e = Yaks_connector.Global.Actual.get_catalog_atomic_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id e state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
            Lwt.return e.id
          )
      in
      let%lwt _ = Lwt_list.iter_p (
          fun (e:User.Descriptors.Entity.constituent_atomic_entity) ->
            match List.find_opt (fun x -> (String.compare x e.id)==0) aes with
            | Some _ -> Lwt.return_unit
            | None -> Lwt.fail @@ FException (`NotFound (`MsgCode ((Printf.sprintf ("Atomic Entity %s not in catalog")e.id),404)))
        ) descriptor.atomic_entities
      in
      (*  *)
      Yaks_connector.Global.Actual.add_catalog_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get descriptor.uuid) descriptor state.yaks
      >>= fun _ ->
      let js = JSON.of_string (User.Descriptors.Entity.string_of_descriptor descriptor) in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-ONBOARD-ENTITY - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* Entity offload from Catalog *)
  let eval_offload_entity self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-OFFLOAD-ENTITY - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-OFFLOAD-ENTITY - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let ae_id = Apero.Option.get @@ Apero.Properties.get "entity_id" props in
    try%lwt

      let%lwt descriptor = Yaks_connector.Global.Actual.get_catalog_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id ae_id state.yaks in
      match descriptor with
      | Some d ->
        Yaks_connector.Global.Actual.remove_catalog_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id d.id state.yaks
        >>= fun _ ->
        let js = JSON.of_string (User.Descriptors.Entity.string_of_descriptor d) in
        let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
      | None ->
        Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Entity %s not in catalog") ae_id ),404)))
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-OFFLOAD-ENTITY - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* Atomic Entity Onboard in Catalog -- this may be moved to Orchestration part *)
  let eval_onboard_ae self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ONBOARD-AE - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ONBOARD-AE - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let descriptor = Apero.Option.get @@ Apero.Properties.get "descriptor" props in
    try%lwt
      let descriptor = User.Descriptors.AtomicEntity.descriptor_of_string descriptor in
      let descriptor =
        match descriptor.uuid with
        | Some _ -> descriptor
        | None ->
          let fduid = Apero.Uuid.to_string @@ Apero.Uuid.make_from_alias descriptor.id in
          {descriptor with uuid = Some fduid}
      in
      (* Adding UUID to CPs *)
      let cps = List.map (fun  (cp:User.Descriptors.Network.connection_point_descriptor) ->
          let cp_uuid = Apero.Uuid.to_string (Apero.Uuid.make ()) in
          {cp with uuid = Some cp_uuid}
        ) descriptor.connection_points
      in
      let descriptor = {descriptor with connection_points = cps} in
      Yaks_connector.Global.Actual.add_catalog_atomic_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get descriptor.uuid) descriptor state.yaks
      >>= fun _ ->
      let js = JSON.of_string (User.Descriptors.AtomicEntity.string_of_descriptor descriptor) in
      let eval_res = FAgentTypes.{result = Some js ; error=None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-ONBOARD-AE - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* EVAL Offload AE *)
  let eval_offload_ae self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-OFFLOAD-AE - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-OFFLOAD-AE - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let ae_id = Apero.Option.get @@ Apero.Properties.get "ae_id" props in
    try%lwt

      let%lwt descriptor = Yaks_connector.Global.Actual.get_catalog_atomic_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id ae_id state.yaks in
      match descriptor with
      | Some d ->
        Yaks_connector.Global.Actual.remove_catalog_atomic_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id d.id state.yaks
        >>= fun _ ->
        let js = JSON.of_string (User.Descriptors.AtomicEntity.string_of_descriptor d) in
        let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
      | None ->
        Lwt.fail @@ FException (`NotFound (`MsgCode ((Printf.sprintf ("Atomic Entity %s not found") ae_id ),404)))
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-OFFLOAD-AE - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* EVAL Instantiate an Entity *)
  (* let eval_instantiate_entity self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-INSTANTIATE-ENTITY - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-INSTANTIATE-ENTITY - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let e_uuid = Apero.Option.get @@ Apero.Properties.get "entity_id" props in
    try%lwt
      let%lwt descriptor = Yaks_connector.Global.Actual.get_catalog_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id e_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      (* Check function *)
      let instance_id = Apero.Uuid.to_string @@ Apero.Uuid.make () in
      (* Add UUID to VLs *)
      let%lwt nets = Lwt_list.map_p (fun (ivl:User.Descriptors.Entity.virtual_link_descriptor) ->
          let net_uuid = Apero.Uuid.to_string (Apero.Uuid.make ()) in
          let%lwt cps = Lwt_list.map_p (fun (e:User.Descriptors.Entity.cp_ref) ->
              let r =
                Infra.Descriptors.Entity.{
                  component_id_ref = e.component_id_ref;
                  component_index_ref = e.component_index_ref;
                  cp_id = e.cp_id;
                  has_floating_ip = e.has_floating_ip;
                  uuid = Apero.Uuid.to_string @@ Apero.Uuid.make ();
                  floating_ip_id = None;floating_ip = None;
                }
              in Lwt.return r
            ) ivl.cps
          in
          let record = Infra.Descriptors.Entity.{
              uuid = net_uuid;
              vl_id = ivl.id;
              is_mgmt = ivl.is_mgmt;
              vl_type = ivl.vl_type;
              root_bandwidth = ivl.root_bandwidth;
              leaf_bandwidth = ivl.leaf_bandwidth;
              cps = cps;
              ip_configuration = ivl.ip_configuration;
              overlay = None;
              vni = None;
              mcast_addr = None;
              vlan_id = None;
              face = None
            }
          in Lwt.return record
        ) descriptor.virtual_links
      in
      (* Adding networks *)
      let%lwt netdescs = Lwt_list.map_s (fun (vl:Infra.Descriptors.Entity.virtual_link_record) ->
          (* let cb_gd_net_all self (net:FTypes.virtual_network option) (is_remove:bool) (uuid:string option) = *)
          let ip_conf =
            match vl.ip_configuration with
            | None -> None
            | Some ipc ->
              Some FTypes.{
                  ip_version = ipc.ip_version;
                  subnet = ipc.subnet;
                  gateway = ipc.gateway;
                  dhcp_enable = ipc.dhcp_enable;
                  dhcp_range = ipc.dhcp_range;
                  dns = ipc.dns
                }
          in
          let net_desc = FTypes.{
              uuid = vl.uuid;
              name = vl.vl_id;
              net_type = FTypes.vn_type_of_string (Infra.Descriptors.Entity.string_of_vl_kind (Apero.Option.get_or_default vl.vl_type `ELAN));
              is_mgmt = vl.is_mgmt;
              overlay = vl.overlay;
              vni = vl.vni;
              mcast_addr = vl.mcast_addr;
              vlan_id = vl.vlan_id;
              face = vl.face;
              ip_configuration = ip_conf
            }
          in
          Fos_fim_api.Network.add_network net_desc state.fim_api
          (* >>= fun _ ->
             (* This has to be removed! *)
             Lwt.return @@ Unix.sleep 3 *)
          >>= fun _ -> Lwt.return net_desc
        ) nets
      in
      ignore netdescs;
      let%lwt aes = Yaks_connector.Global.Actual.get_catalog_all_atomic_entities (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id state.yaks  >>=
        Lwt_list.map_p (fun (e:string) ->
            let%lwt e = Yaks_connector.Global.Actual.get_catalog_atomic_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id e state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
            Lwt.return (e.id, Apero.Option.get e.uuid)
          )
      in

      let%lwt ae_instances = Lwt_list.map_p (fun (e:User.Descriptors.Entity.constituent_atomic_entity) ->
          match List.find_opt (fun (x,_) -> (String.compare x e.id)==0) aes with
          | Some (_,ae_uuid) ->
            let%lwt desc = Yaks_connector.Global.Actual.get_catalog_atomic_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id ae_uuid state.yaks in
            (match desc with
             | Some _ ->
               let%lwt ae_rec = Fos_faem_api.AtomicEntity.instantiate ae_uuid state.faem_api in
               Lwt.return @@ Infra.Descriptors.Entity.{
                   id = ae_uuid;
                   uuid = ae_rec.uuid;
                   index = e.index
                 }


             | None -> Lwt.fail @@ FException (`NotFound (`MsgCode ((Printf.sprintf ("Atomic Entity %s not in catalog")e.id),404))))
          | None -> Lwt.fail @@ FException (`NotFound (`MsgCode ((Printf.sprintf ("Atomic Entity %s not in catalog")e.id),404)))
        ) descriptor.atomic_entities
      in
      let%lwt nets = Lwt_list.map_p (fun (vl:Infra.Descriptors.Entity.virtual_link_record) ->
          let%lwt cps = Lwt_list.map_p (fun (cp:Infra.Descriptors.Entity.cp_ref) ->
              (match List.find_opt (fun (ae:Infra.Descriptors.Entity.constituent_atomic_entity) -> ae.index == cp.component_index_ref) ae_instances with
               | Some ae ->
                 let%lwt ae_dec = Fos_faem_api.AtomicEntity.get_atomic_entity_descriptor ae.id state.faem_api in
                 let%lwt ae_rec = Fos_faem_api.AtomicEntity.get_atomic_entity_instance_info ae.uuid state.faem_api in
                 (match List.find_opt (fun (e:User.Descriptors.Network.connection_point_descriptor) -> (String.compare e.id cp.cp_id)==0) ae_dec.connection_points with
                  | Some ae_cpd ->
                    ( match List.find_opt (fun (e:Infra.Descriptors.Network.connection_point_record) ->  (String.compare e.cp_id (Apero.Option.get ae_cpd.uuid)==0)) ae_rec.connection_points with
                      | Some ae_cpr ->
                        let%lwt cp_node = Fos_fim_api.Network.get_node_from_connection_point ae_cpr.cp_id state.fim_api in
                        (match cp_node with
                         |Some cp_node ->
                           (* Find Network associated with this VL *)
                           (match List.find_opt (fun (e:FTypes.virtual_network) -> (String.compare e.uuid vl.uuid)==0 ) netdescs with
                            | Some ndesc ->
                              (* Adding network to node *)
                              let%lwt  _ = Fos_fim_api.Network.add_network_to_node ndesc cp_node state.fim_api in
                              (* Connecting CP to network *)
                              let%lwt _ = Fos_fim_api.Network.connect_cp_to_network ae_cpr.uuid ndesc.uuid cp_node state.fim_api in
                              (* Updating Record with correct CP ID  *)
                              Lwt.return {cp with uuid = ae_cpr.cp_id}
                            | None -> Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find a network associated to this VL %s") vl.uuid ),404) ))
                           )
                         | None -> Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find an node for cp %s:%s")  ae_cpr.cp_id ae_cpr.uuid ),404) ))
                        )
                      (* Fos_fim_api.Network.add_network_to_node *)
                      | None -> Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find an instance for CP %s in AE Record %s") cp.cp_id ae_rec.uuid ),404) ))
                    )

                  | None -> Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find the CP %s in AE %s") cp.cp_id ae_dec.id ),404) ))
                 )

               | None -> Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find the AE for CP %s") cp.cp_id ),404) ))
              )
            ) vl.cps
          in
          Lwt.return {vl with cps = cps}

        ) nets
      in
      let record = Infra.Descriptors.Entity.{
          uuid = instance_id;
          entity_id = e_uuid;
          atomic_entities = ae_instances;
          virtual_links = nets;
        }
      in
      let js = JSON.of_string (Infra.Descriptors.Entity.string_of_record record) in
      let%lwt _ = Yaks_connector.Global.Actual.add_records_entity_instance_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id e_uuid instance_id record state.yaks in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-INSTANTIATE-ENTITY - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
  in *)
  (* EVAL Terminate an AE *)
  (* let eval_terminate_entity self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-TERMINATE-AE - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-TERMINATE-AE - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    (* let ae_id = Apero.Option.get @@ Apero.Properties.get "ae_id" props in *)
    let e_instance_id = Apero.Option.get @@ Apero.Properties.get "instance_id" props in
    try%lwt
      let%lwt record = Yaks_connector.Global.Actual.get_records_entity_instance_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id "*" e_instance_id state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let%lwt _ = Lwt_list.iter_p (fun (ae:Infra.Descriptors.Entity.constituent_atomic_entity) ->
          Fos_faem_api.AtomicEntity.terminate ae.uuid state.faem_api
          >>= fun _ -> Lwt.return_unit
        ) record.atomic_entities
      in
      let%lwt _ = Lwt_list.iter_p (fun (vlr:Infra.Descriptors.Entity.virtual_link_record) ->
          let%lwt _ = Fos_fim_api.Network.remove_network vlr.uuid state.fim_api in
          Lwt.return_unit
        ) record.virtual_links
      in
      let js = JSON.of_string (Infra.Descriptors.Entity.string_of_record record) in
      let%lwt _ = Yaks_connector.Global.Actual.remove_records_atomic_entity_instance_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id record.entity_id e_instance_id state.yaks in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-TERMINATE-ENTITY - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
  in *)
  (* EVAL Instantiate an AE  *)
  let eval_instantiate_ae self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-INSTANTIATE-AE - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-INSTANTIATE-AE - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let ae_uuid = Apero.Option.get @@ Apero.Properties.get "ae_id" props in
    try%lwt
      let%lwt descriptor = Yaks_connector.Global.Actual.get_catalog_atomic_entity_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id ae_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      (* Check function *)
      let check_fdu_nodes_compatibility sysid tenantid fdu_info =
        let parameters = [("descriptor",User.Descriptors.FDU.string_of_descriptor fdu_info)] in
        let fname = "check_node_compatibilty" in
        Yaks_connector.Global.Actual.exec_multi_node_eval sysid tenantid fname parameters state.yaks
      in
      (* Add UUID to VLs *)
      let%lwt nets = Lwt_list.map_p (fun (ivl:User.Descriptors.AtomicEntity.internal_virtual_link_descriptor) ->
          let net_uuid = Apero.Uuid.to_string (Apero.Uuid.make ()) in
          let record = Infra.Descriptors.AtomicEntity.{
              uuid = net_uuid;
              internal_vl_id = ivl.id;
              is_mgmt = ivl.is_mgmt;
              vl_type = ivl.vl_type;
              root_bandwidth = ivl.root_bandwidth;
              leaf_bandwidth = ivl.leaf_bandwidth;
              int_cps = ivl.int_cps;
              ip_configuration = ivl.ip_configuration;
              overlay = None;
              vni = None;
              mcast_addr = None;
              vlan_id = None;
              face = None
            }
          in Lwt.return record
        ) descriptor.internal_virtual_links
      in
      (* let%lwt cps = Lwt_list.map_p (fun (cp:User.Descriptors.Network.connection_point_descriptor) ->
          let cp_uuid = Apero.Uuid.to_string (Apero.Uuid.make ()) in
          let record = Infra.Descriptors.Network.{
              status = `CREATE;
              uuid = cp_uuid;
              cp_id = cp.id;
              cp_type = cp.cp_type;
              port_security_enabled = cp.port_security_enabled;
              veth_face_name = None; br_name = None;  properties = None;
              vld_ref = cp.vld_ref
            }
          in Lwt.return record
         ) descriptor.connection_points
         in *)
      (* update the FDU with correct connection to the VLs *)
      (* Pretend we already ordered the fdus *)
      let ordered_fdus = descriptor.fdus in
      (* We assing a UUID to the FDUs *)
      let%lwt fdus = Lwt_list.map_p (fun (fdu:User.Descriptors.FDU.descriptor) ->  Lwt.return {fdu with uuid = Some (Apero.Uuid.to_string (Apero.Uuid.make ())) }) ordered_fdus in
      (* update FDUs with correct id for VLs *)
      (* let%lwt fdus = Lwt_list.map_p (fun (fdu:User.Descriptors.FDU.descriptor) ->

          let%lwt cps = Lwt_list.map_p (fun (cp:User.Descriptors.Network.connection_point_descriptor) ->
              let ivl = List.find_opt (fun (vl:Infra.Descriptors.AtomicEntity.internal_virtual_link_record) ->
                  match List.find_opt (fun id -> (String.compare id cp.id) == 0) vl.int_cps with
                  | Some _ -> true
                  | None -> false
                ) nets in
              match ivl with
              | Some vl -> Lwt.return {cp with vld_ref = Some vl.uuid}
              | None -> Lwt.return cp
            )fdu.connection_points
          in
          let fdu = {fdu with connection_points = cps} in
          Lwt.return fdu
         )  fdus
         in *)
      (* update the descriptor with ordered FDUs, FDU UUIDs, and VLs connections *)
      let descriptor = {descriptor with fdus = fdus} in
      (* Get compatible nodes for each FDU *)
      let%lwt fdus_node_maps = Lwt_list.map_p (fun (fdu:User.Descriptors.FDU.descriptor) ->
          let%lwt res = check_fdu_nodes_compatibility (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id fdu in
          match res with
          | [] -> Lwt.fail @@ FException (`NoCompatibleNodes (`MsgCode (( Printf.sprintf ("No Node found compatible with this FDU %s") fdu.id ),503) ))
          | lst ->
            let%lwt lst = Lwt_list.filter_map_p (fun (e:FAgentTypes.eval_result) ->
                match e.result with
                | Some r ->
                  let r = (FAgentTypes.compatible_node_response_of_string (JSON.to_string r)) in
                  (match r.is_compatible with
                   | true ->  Lwt.return (Some r.uuid)
                   | false -> Lwt.return None)
                | None -> Lwt.return None
              ) lst
            in
            match lst with
            | [] -> Lwt.fail @@ FException (`NoCompatibleNodes (`MsgCode ((Printf.sprintf ("No Node found compatible with this FDU %s") fdu.id ),503)))
            | l -> Lwt.return (fdu, l)

        ) descriptor.fdus
      in
      (* Instantiating Virtual Networks *)
      let%lwt netdescs = Lwt_list.map_p (fun (vl:Infra.Descriptors.AtomicEntity.internal_virtual_link_record) ->
          (* let cb_gd_net_all self (net:FTypes.virtual_network option) (is_remove:bool) (uuid:string option) = *)
          let ip_conf =
            match vl.ip_configuration with
            | None -> None
            | Some ipc ->
              Some FTypes.{
                  ip_version = ipc.ip_version;
                  subnet = ipc.subnet;
                  gateway = ipc.gateway;
                  dhcp_enable = ipc.dhcp_enable;
                  dhcp_range = ipc.dhcp_range;
                  dns = ipc.dns
                }
          in
          let net_desc = FTypes.{
              uuid = vl.uuid;
              name = vl.internal_vl_id;
              net_type = FTypes.vn_type_of_string (Base.Descriptors.Network.string_of_vl_kind (Apero.Option.get_or_default vl.vl_type `ELAN));
              is_mgmt = vl.is_mgmt;
              overlay = vl.overlay;
              vni = vl.vni;
              mcast_addr = vl.mcast_addr;
              vlan_id = vl.vlan_id;
              face = vl.face;
              ip_configuration = ip_conf
            }
          in
          Fos_fim_api.Network.add_network net_desc state.fim_api
          (* >>= fun _ ->
             (* This has to be removed! *)
             Lwt.return @@ Unix.sleep 3 *)
          >>= fun _ -> Lwt.return net_desc
        ) nets
      in
      (* Lwt_list.iter_s (fun (cp:Infra.Descriptors.Network.connection_point_record) ->
          let fdu_cp = User.Descriptors.Network.{
              name = cp.cp_id;
              id = cp.cp_id;
              cp_type = cp.cp_type;
              port_security_enabled = cp.port_security_enabled;
              uuid = None;
              short_name = None;
              vld_ref = cp.vld_ref;
            }
          in
          Fos_fim_api.Network.add_connection_point fdu_cp state.fim_api
          >>= fun _ ->
          Lwt.return_unit
         ) cps
         >>= fun _ -> *)
      (* Onboard and Instantiate FDUs descriptors *)
      let%lwt fdurs = Lwt_list.map_p ( fun ((fdu:User.Descriptors.FDU.descriptor),(nodes:string list)) ->
          let n = List.nth nodes (Random.int (List.length nodes)) in
          let%lwt _ = Fos_fim_api.FDU.onboard fdu state.fim_api in

          let%lwt fdur = Fos_fim_api.FDU.define (Apero.Option.get fdu.uuid) n state.fim_api in

          let%lwt _ = Fos_fim_api.FDU.configure fdur.uuid state.fim_api  in
          (* Connecting FDU interface to right connection points *)
          let%lwt cprs  = Lwt_list.filter_map_s (fun (iface:Infra.Descriptors.FDU.interface) ->
              match iface.ext_cp_id, iface.cp_id with
              | Some ecp, None ->
                (match List.find_opt (fun (e:User.Descriptors.Network.connection_point_descriptor) -> (String.compare ecp e.id)==0) descriptor.connection_points with
                 | Some ae_ecp ->
                   let%lwt cpr = Fos_fim_api.Network.add_connection_point_to_node ae_ecp n state.fim_api in
                   let _ = Logs.info (fun m -> m "[FOS-AGENT] - EV-INSTANTIATE-AE - CONNECTING A INTERFACE TO AN EXTERNAL CP : %s <-> %s:%s" cpr.uuid fdur.uuid iface.name ) in
                   Fos_fim_api.FDU.connect_interface_to_cp cpr.uuid fdur.uuid iface.name n state.fim_api
                   >>= fun _ -> Lwt.return (Some cpr)
                 | None ->Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find Atomic entity connection point %s") ecp ),404) ))
                )
              | None, Some icp ->
                (match List.find_opt (fun (e:Infra.Descriptors.FDU.connection_point_record) -> (String.compare icp e.uuid)==0) fdur.connection_points with
                 | Some fdu_icp ->
                   (match fdu_icp.vld_ref with
                    | Some vlr ->
                      (match List.find_opt (fun (e:Infra.Descriptors.AtomicEntity.internal_virtual_link_record) -> (String.compare e.internal_vl_id vlr)==0 ) nets with
                       | Some vl ->
                         ( match List.find_opt (fun (e:FTypes.virtual_network) -> (String.compare vl.uuid e.uuid)==0) netdescs with
                           | Some net_desc ->
                             let%lwt vnet_r = Fos_fim_api.Network.add_network_to_node net_desc n state.fim_api in
                             Fos_fim_api.Network.connect_cp_to_network fdu_icp.uuid vnet_r.uuid n state.fim_api
                             >>= fun _ -> Lwt.return None
                           | None -> Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find virtual network %s") vl.uuid ),404) ))
                         )
                       | None -> Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find virtual link %s") vlr ),404) ))
                      )
                    | None -> Lwt.return None
                   )
                 (* Fos_fim_api.Network.add_network_to_node
                    Fos_fim_api.FDU.connect_interface_to_cp cpr.uuid fdur.uuid iface.name n state.fim_api
                    >>= fun _ -> Lwt.return_unit *)
                 | None -> Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find FDU connection point %s") icp ),404) ))
                )
              | _, _ ->  Lwt.return  None

            ) fdur.interfaces
          in
          let%lwt _ = Fos_fim_api.FDU.start fdur.uuid state.fim_api  in
          Lwt.return (fdur, cprs)
        ) fdus_node_maps
      in
      let instanceid = Apero.Uuid.to_string (Apero.Uuid.make ()) in
      let cprs = List.map (fun (_,cprs) -> cprs) fdurs in
      let cprs = List.flatten cprs in
      let fdurs = List.map (fun (fdur,_) -> fdur) fdurs in
      let ae_record = Infra.Descriptors.AtomicEntity.{
          uuid = instanceid;
          fdus = fdurs;
          atomic_entity_id = ae_uuid;
          internal_virtual_links = nets;
          connection_points = cprs;
          depends_on = descriptor.depends_on
        }
      in
      let js = JSON.of_string (Infra.Descriptors.AtomicEntity.string_of_record ae_record) in
      let%lwt _ = Yaks_connector.Global.Actual.add_records_atomic_entity_instance_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id ae_uuid instanceid ae_record state.yaks in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-INSTANTIATE-AE - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* EVAL Terminate an AE *)
  let eval_terminate_ae self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-TERMINATE-AE - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-TERMINATE-AE - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    (* let ae_id = Apero.Option.get @@ Apero.Properties.get "ae_id" props in *)
    let ae_instance_id = Apero.Option.get @@ Apero.Properties.get "instance_id" props in
    try%lwt
      let%lwt record = Yaks_connector.Global.Actual.get_records_atomic_entity_instance_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id "*" ae_instance_id state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let%lwt _ = Lwt_list.iter_p (fun (fdur:Infra.Descriptors.FDU.record) ->
          Fos_fim_api.FDU.terminate fdur.uuid state.fim_api
          >>= fun _ -> Fos_fim_api.FDU.offload fdur.fdu_id state.fim_api
          >>= fun _ -> Lwt.return_unit
        ) record.fdus
      in
      let%lwt _ = Lwt_list.iter_p (fun (vlr:Infra.Descriptors.AtomicEntity.internal_virtual_link_record) ->
          Fos_fim_api.Network.remove_network vlr.uuid state.fim_api
          >>= fun _ -> Lwt.return_unit
        ) record.internal_virtual_links
      in
      let%lwt _ = Lwt_list.iter_s (fun (cp:Infra.Descriptors.Network.connection_point_record) ->
          let%lwt cp_node = Fos_fim_api.Network.get_node_from_connection_point cp.cp_id state.fim_api in
          (match cp_node with
           |Some cp_node ->
             let%lwt _ = Fos_fim_api.Network.remove_connection_point_from_node cp.uuid cp_node state.fim_api in
             Lwt.return_unit
           | None -> Lwt.fail @@ FException (`NotFound (`MsgCode (( Printf.sprintf ("Unable to find a node for this CP %s") cp.uuid ),404) ))
          )
        ) record.connection_points
      in
      let js = JSON.of_string (Infra.Descriptors.AtomicEntity.string_of_record record) in
      let%lwt _ = Yaks_connector.Global.Actual.remove_records_atomic_entity_instance_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id record.atomic_entity_id ae_instance_id state.yaks in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-TERMINATE-AE - EXCEPTION: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*     NM Floating IPs *)
  let eval_create_floating_ip self (props:Apero.properties) =
    ignore props;
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-NEW-FLOATING-IP - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-NEW-FLOATING-IP - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-NEW-FLOATING-IP - # NetManager: %s" net_p) in
    try%lwt
      let fname = "create_floating_ip" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname [] state.yaks
      >>= fun res ->
      match res with
      | Some r ->
        let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-NEW-FLOATING-IP - GOT RESPONSE FROM EVAL %s" (FAgentTypes.string_of_eval_result r)) in
        (* Convertion from record *)
        let floating_r = FTypes.floating_ip_record_of_string @@ JSON.to_string (Apero.Option.get r.result) in
        let floating = FTypes.{uuid = floating_r.uuid; ip_version = floating_r.ip_version; address = floating_r.address} in
        Yaks_connector.Global.Actual.add_node_floating_ip (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get state.configuration.agent.uuid) floating.uuid floating state.yaks
        >>= fun _ ->
        let eval_res = FAgentTypes.{result = Some (JSON.of_string (FTypes.string_of_floating_ip floating)) ; error = None; error_msg = None} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
      |  None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ("Cannot create floating ip %s not found",503)))
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error = Some 22; error_msg = Some (Printexc.to_string exn)} in
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-NEW-FLOATING-IP - # ERROR WHEN CREATING FLOATING IP") in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_delete_floating_ip self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEL-FLOATING-IP - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEL-FLOATING-IP - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEL-FLOATING-IP - # NetManager: %s" net_p) in
    try%lwt
      let ip_id = Apero.Option.get @@ Apero.Properties.get "floating_uuid" props in
      let parameters = [("ip_id",ip_id)] in
      let fname = "delete_floating_ip" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
        Some r ->
        let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEL-FLOATING-IP - GOT RESPONSE FROM EVAL %s" (FAgentTypes.string_of_eval_result r)) in
        (* Convertion from record *)
        let floating_r = FTypes.floating_ip_record_of_string @@ JSON.to_string (Apero.Option.get r.result) in
        let floating = FTypes.{uuid = floating_r.uuid; ip_version = floating_r.ip_version; address = floating_r.address} in
        Yaks_connector.Global.Actual.add_node_floating_ip (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get state.configuration.agent.uuid) floating.uuid floating state.yaks
        >>= fun _ ->
        let eval_res = FAgentTypes.{result = Some (JSON.of_string (FTypes.string_of_floating_ip floating)) ; error = None; error_msg =  None} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
      |  None ->  Lwt.fail @@ FException (`NotFound (`MsgCode ((Printf.sprintf ("Floating IP %s not found") ip_id ),404)))
    with
      e ->
      let msg = Printexc.to_string e
      and stack = Printexc.get_backtrace () in
      Printf.eprintf "there was an error: %s%s\n" msg stack;
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-NEW-FLOATING-IP - # ERROR WHEN DELETING FLOATING IP") in
      let eval_res = FAgentTypes.{result = None ; error = Some 22; error_msg = Some (Printexc.to_string e)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_assign_floating_ip self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ASSOC-FLOATING-IP - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ASSOC-FLOATING-IP - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    try%lwt
      let ip_id = Apero.Option.get @@ Apero.Properties.get "floating_uuid" props in
      let cp_id = Apero.Option.get @@ Apero.Properties.get "cp_uuid" props in
      let parameters = [("ip_id",ip_id);("cp_id",cp_id)] in
      let fname = "assign_floating_ip" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
      | Some r ->
        (* Convertion from record *)
        let floating_r = FTypes.floating_ip_record_of_string @@ JSON.to_string (Apero.Option.get r.result) in
        let floating = FTypes.{uuid = floating_r.uuid; ip_version = floating_r.ip_version; address = floating_r.address} in
        Yaks_connector.Global.Actual.add_node_floating_ip (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get state.configuration.agent.uuid) floating.uuid floating state.yaks
        >>= fun _ ->
        let eval_res = FAgentTypes.{result = Some (JSON.of_string (FTypes.string_of_floating_ip floating)) ; error=None; error_msg = None} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
      |  None ->  Lwt.fail @@ FException (`NotFound (`MsgCode ((Printf.sprintf ("Cannot assing IP %s to cp %s") ip_id cp_id ),503)))
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error = Some 33; error_msg = Some (Printexc.to_string exn)} in
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-ASSOC-FLOATING-IP - EXCEPTION: %s" (Printexc.to_string exn)) in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_remove_floating_ip self (props:Apero.properties) =
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-REMOVE-FLOATING-IP - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-REMOVE-FLOATING-IP - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    try%lwt
      let ip_id = Apero.Option.get @@ Apero.Properties.get "floating_uuid" props in
      let cp_id = Apero.Option.get @@ Apero.Properties.get "cp_uuid" props in
      let parameters = [("ip_id",ip_id);("cp_id",cp_id)] in
      let fname = "remove_floating_ip" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
      | Some r ->
        (* Convertion from record *)
        let floating_r = FTypes.floating_ip_record_of_string @@ JSON.to_string (Apero.Option.get r.result) in
        let floating = FTypes.{uuid = floating_r.uuid; ip_version = floating_r.ip_version; address = floating_r.address} in
        Yaks_connector.Global.Actual.add_node_floating_ip (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get state.configuration.agent.uuid) floating.uuid floating state.yaks
        >>= fun _ ->
        let eval_res = FAgentTypes.{result = Some (JSON.of_string (FTypes.string_of_floating_ip floating)) ; error = None; error_msg = None} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
      |  None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot remove floating IP %s not found") ip_id ),503)))
    with
    | exn ->
      let eval_res = FAgentTypes.{result = None ; error = Some 33; error_msg = Some (Printexc.to_string exn)} in
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-REMOVE-FLOATING-IP - EXCEPTION: %s" (Printexc.to_string exn)) in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_add_router_port self (props:Apero.properties) =
    ignore props;
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ADD-ROUTER-PORT - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ADD-ROUTER-PORT - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ADD-ROUTER-PORT - # NetManager: %s" net_p) in
    try%lwt
      let fname = "add_router_port" in
      let rid = Apero.Option.get @@ Apero.Properties.get "router_id" props in
      let port_type = Apero.Option.get @@ Apero.Properties.get "port_type" props in
      let parameters = [("router_id", rid); ("port_type", port_type)] in
      let parameters =
        match Apero.Properties.get "vnet_id" props with
        | Some vid -> parameters @ [("vnet_id",vid)]
        | None -> parameters
      in
      let parameters =
        match Apero.Properties.get "ip_address" props with
        | Some ip -> parameters @ [("ip_address",ip)]
        | None -> parameters
      in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
      | Some r ->
        let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-ADD-ROUTER-PORT - GOT RESPONSE FROM EVAL %s" (FAgentTypes.string_of_eval_result r)) in
        (* Convertion from record *)
        let router = Router.record_of_string @@ JSON.to_string (Apero.Option.get r.result) in
        let%lwt ports = Lwt_list.map_p (fun (e:Router.router_port_record) ->
            Lwt.return Router.{port_type = e.port_type; vnet_id = e.vnet_id; ip_address = Some e.ip_address}
          ) router.ports
        in
        let router_desc = Router.{uuid = Some router.uuid; ports = ports; } in
        (*  *)
        Yaks_connector.Global.Actual.add_node_router (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id  (Apero.Option.get state.configuration.agent.uuid) router.uuid router_desc state.yaks
        >>= fun _ ->
        let eval_res = FAgentTypes.{result = Some (JSON.of_string (Router.string_of_record router)) ; error = None; error_msg = None} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
      |  None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot create to router  %s") rid ),503)))
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-ADD-ROUTER-PORT - # ERROR WHEN ADDING ROUTER PORT: %s" (Printexc.to_string exn) ) in
      let eval_res = FAgentTypes.{result = None ; error = Some 22; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_remove_router_port self (props:Apero.properties) =
    ignore props;
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEL-ROUTER-PORT - ##############") in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEL-ROUTER-PORT - Properties: %s" (Apero.Properties.to_string props) ) in
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEL-ROUTER-PORT - # NetManager: %s" net_p) in
    try%lwt
      let fname = "remove_router_port" in
      let rid = Apero.Option.get @@ Apero.Properties.get "router_id" props in
      let vid = Apero.Option.get @@ Apero.Properties.get "vnet_id" props in
      let parameters = [("router_id", rid); ("vnet_id", vid)] in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
      | Some r ->
        let _ = Logs.debug (fun m -> m "[FOS-AGENT] - EV-DEL-ROUTER-PORT - GOT RESPONSE FROM EVAL %s" (FAgentTypes.string_of_eval_result r)) in
        (* Convertion from record *)
        let router = Router.record_of_string @@ JSON.to_string (Apero.Option.get r.result) in
        let%lwt ports = Lwt_list.map_p (fun (e:Router.router_port_record) ->
            Lwt.return Router.{port_type = e.port_type; vnet_id = e.vnet_id; ip_address = Some e.ip_address}
          ) router.ports
        in
        let router_desc = Router.{uuid = Some router.uuid; ports = ports; } in
        (*  *)
        Yaks_connector.Global.Actual.add_node_router (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id  (Apero.Option.get state.configuration.agent.uuid) router.uuid router_desc state.yaks
        >>= fun _ ->
        let eval_res = FAgentTypes.{result = Some (JSON.of_string (Router.string_of_record router)) ; error = None; error_msg = None} in
        Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
      |  None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot remove port from router %s") rid ),503)))
    with
    | exn ->
      let _ = Logs.err (fun m -> m "[FOS-AGENT] - EV-DEL-ROUTER-PORT - # ERROR WHEN REMOVING ROUTER PORT: %s" (Printexc.to_string exn)) in
      let eval_res = FAgentTypes.{result = None ; error = Some 22; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
