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
      Logs.debug (fun m -> m "[eval_get_node_fdu_info] - Search for FDU Info");
      let%lwt descriptor = Yaks_connector.Global.Actual.get_node_fdu_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id node_uuid fdu_uuid instanceid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let js = FAgentTypes.json_of_string @@ Infra.Descriptors.FDU.string_of_record  descriptor in
      Logs.debug (fun m -> m "[eval_get_node_fdu_info] - INFO %s" (FAgentTypes.string_of_json js));
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
    Logs.debug (fun m -> m "[eval_get_port_info] - Getting info for port %s" cp_uuid );
    try%lwt
      let%lwt descriptor = Yaks_connector.Global.Actual.get_port (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id cp_uuid state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      let js = FAgentTypes.json_of_string @@ User.Descriptors.Network.string_of_connection_point_descriptor  descriptor in
      let eval_res = FAgentTypes.{result = Some js ; error = None; error_msg = None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
    with
    | _ ->
      Logs.debug (fun m -> m "[eval_get_port_info] - Search port on FDU");
      let%lwt fdu_ids = Yaks_connector.Global.Actual.get_catalog_all_fdus (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id state.yaks in
      let%lwt cps = Lwt_list.filter_map_p (fun e ->
          let%lwt fdu =  Yaks_connector.Global.Actual.get_catalog_fdu_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id e state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
          let%lwt c = Lwt_list.filter_map_p (fun (cp:User.Descriptors.Network.connection_point_descriptor) ->
              Logs.debug (fun m -> m "[eval_get_port_info] - %s == %s ? %d " cp.id cp_uuid (String.compare cp.id  cp_uuid));
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
      Logs.debug (fun m -> m "[eval_create_net] - ##############");
      Logs.debug (fun m -> m "[eval_create_net] - Properties: %s" (Apero.Properties.to_string props) );
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
      Logs.err (fun m -> m "[eval_create_net] - Exception: %s Trace %s " (Printexc.to_string exn) bt);
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_remove_net self (props:Apero.properties) =
    MVar.read self >>= fun state ->
    try%lwt
      let%lwt net_p = get_network_plugin self in
      Logs.debug (fun m -> m "[eval_remove_net] - ##############");
      Logs.debug (fun m -> m "[eval_remove_net] - Properties: %s" (Apero.Properties.to_string props));
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
      Logs.err (fun m -> m "[eval_remove_net] - EXCEPTION: %s" (Printexc.to_string exn));
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_create_cp self (props:Apero.properties) =
    Logs.debug (fun m -> m "[eval_create_cp] - ##############");
    Logs.debug (fun m -> m "[eval_create_cp] - Properties: %s" (Apero.Properties.to_string props));
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let descriptor = User.Descriptors.Network.connection_point_descriptor_of_string @@ Apero.Option.get @@ Apero.Properties.get "descriptor" props in
    Logs.debug (fun m -> m "[eval_create_cp] - # NetManager: %s" net_p);
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
      Logs.err (fun m -> m "[eval_create_cp] Exception : %s" (Printexc.to_string exn));
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_remove_cp self (props:Apero.properties) =
    Logs.debug (fun m -> m "[eval_remove_cp] - ##############");
    Logs.debug (fun m -> m "[eval_remove_cp] - Properties: %s" (Apero.Properties.to_string props));
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let cp_id =  Apero.Option.get @@ Apero.Properties.get "cp_id" props in
    Logs.debug (fun m -> m "[eval_remove_cp] - # NetManager: %s" net_p);
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
      Logs.err (fun m -> m "[eval_remove_cp] Exception %s" (Printexc.to_string exn));
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_connect_cp_to_fdu_face self (props:Apero.properties) =
    Logs.debug (fun m -> m "[eval_connect_cp_to_fdu_face] - ##############");
    Logs.debug (fun m -> m "[eval_connect_cp_to_fdu_face] - Properties: %s" (Apero.Properties.to_string props));
    MVar.read self >>= fun state ->
    let cp_id = Apero.Option.get @@ Apero.Properties.get "cp_id" props in
    let instance_id = Apero.Option.get @@ Apero.Properties.get "instance_id" props in
    let interface = Apero.Option.get @@ Apero.Properties.get "interface" props in
    try%lwt

      let%lwt record = Yaks_connector.Global.Actual.get_node_fdu_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get state.configuration.agent.uuid) "*" instance_id state.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
      Logs.debug (fun m -> m "[[eval_connect_cp_to_fdu_face] - FDU Record: %s" (Infra.Descriptors.FDU.string_of_record record));
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
          Logs.err (fun m -> m "Cannot find a plugin for this FDU even if it is present in the node WTF!! %s" instance_id );
          None
        | _ -> Some  ((List.hd matching_plugins).uuid)
      in
      (* Create Record
       * Add UUID for each component
       * Fix references with UUIDs
      *)
      (match pl with
       | Some plid ->
         Logs.debug (fun m -> m "[eval_connect_cp_to_fdu_face] - Plugin ID: %s" plid);
         let parameters = [("cpid", cp_id);("instanceid", instance_id);("iface",interface)] in
         let fname = "connect_interface_to_cp" in
         Yaks_connector.Local.Actual.exec_plugin_eval (Apero.Option.get state.configuration.agent.uuid) plid fname parameters state.yaks
         >>= fun res ->
         (match res with
          | Some r ->
            Logs.debug (fun m -> m "[eval_connect_cp_to_fdu_face] - Connected!");
            Lwt.return @@ FAgentTypes.string_of_eval_result r
          | None ->  Lwt.fail @@ FException (`InternalError (`MsgCode ((Printf.sprintf ("Cannot connect cp to interface %s") cp_id ),503)))
         )
       | None ->
         Lwt.fail @@ FException (`PluginNotFound (`MsgCode ((Printf.sprintf ("CRITICAL!!!! Cannot find a plugin for this FDU even if it is present in the node!! %s") instance_id ),404))))
    with
    | exn ->
      Logs.err (fun m -> m "[eval_connect_cp_to_fdu_face] Exception: %s" (Printexc.to_string exn));
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_disconnect_cp_from_fdu_face self (props:Apero.properties) =
    Logs.debug (fun m -> m "[[eval_disconnect_cp_from_fdu_face] - ##############");
    Logs.debug (fun m -> m "[[eval_disconnect_cp_from_fdu_face] - Properties: %s" (Apero.Properties.to_string props));
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
          Logs.err (fun m -> m "[eval_disconnect_cp_from_fdu_face] - Cannot find a plugin for this FDU even if it is present in the node WTF!! %s" instance_id );
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
      Logs.err (fun m -> m "[eval_disconnect_cp_from_fdu_face] - EXCEPTION: %s" (Printexc.to_string exn));
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_connect_cp_to_network self (props:Apero.properties) =
    Logs.debug (fun m -> m "[eval_connect_cp_to_network] - ##############");
    Logs.debug (fun m -> m "[eval_connect_cp_to_network] - Properties: %s" (Apero.Properties.to_string props));
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let cp_id = Apero.Option.get @@ Apero.Properties.get "cp_uuid" props in
    let net_id = Apero.Option.get @@ Apero.Properties.get "network_uuid" props in
    Logs.debug (fun m -> m "[eval_connect_cp_to_network] - # NetManager: %s" net_p);
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
    Logs.debug (fun m -> m "[eval_remove_cp_from_network] - ##############");
    Logs.debug (fun m -> m "[eval_remove_cp_from_network] - Properties: %s" (Apero.Properties.to_string props));
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    let cp_id = Apero.Option.get @@ Apero.Properties.get "cp_uuid" props in
    Logs.debug (fun m -> m "[eval_remove_cp_from_network] - # NetManager: %s" net_p);
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

  (* FDU Onboard in Catalog -- this may be moved to FOrcE *)
  let eval_onboard_fdu self (props:Apero.properties) =
    Logs.debug (fun m -> m "[[eval_onboard_fdu] - ##############");
    Logs.debug (fun m -> m "[eval_onboard_fdu] - Properties: %s" (Apero.Properties.to_string props));
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
      Logs.err (fun m -> m "[eval_onboard_fdu] - EXCEPTION: %s" (Printexc.to_string exn));
      let eval_res = FAgentTypes.{result = None ; error = Some 11; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (* FDU Definition in Node *)
  let eval_define_fdu self (props:Apero.properties) =
    Logs.debug (fun m -> m "[eval_define_fdu] - ##############");
    Logs.debug (fun m -> m "[eval_define_fdu] - Properties: %s" (Apero.Properties.to_string props));
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
            Logs.debug (fun m -> m "[eval_define_fdu] - THIS FDU HAS PHYSICAL INTERFACE");
            let  r = Infra.Descriptors.FDU.{name = e.name; is_mgmt = e.is_mgmt; if_type = e.if_type;
                                            mac_address = e.mac_address; virtual_interface = e.virtual_interface;
                                            cp_id = cp_new_id; ext_cp_id = e.ext_cp_id;
                                            vintf_name = e.name; status = `CREATE; phy_face = Some e.virtual_interface.vpci;
                                            veth_face_name = None; properties = None}
            in
            Logs.debug (fun m -> m "[eval_define_fdu] - THIS FDU HAS PHYSICAL INTERFACE RECORD: %s" (Infra.Descriptors.FDU.string_of_interface r));
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
      Logs.err (fun m -> m "[eval_define_fdu] - Exception: %s" (Printexc.to_string exn));
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
    Logs.debug (fun m -> m "[eval_check_fdu] - ##############");
    Logs.debug (fun m -> m "[eval_check_fdu] - Properties: %s" (Apero.Properties.to_string props));
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
        Logs.debug (fun m -> m "[eval_check_fdu] - CPU Arch Check: %s = %s ? %b" fdu_cp.cpu_arch ncpu.arch ((String.compare fdu_cp.cpu_arch ncpu.arch) == 0));
        Logs.debug (fun m -> m "[eval_check_fdu] - RAM Size Check: %b" (fdu_cp.ram_size_mb <= ninfo.ram.size));
        Logs.debug (fun m -> m "[eval_check_fdu] - Plugin Check: %b" has_plugin );
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
      Logs.err (fun m -> m "[eval_check_fdu] - Exception: %s" (Printexc.to_string exn));
      let eval_res = FAgentTypes.{result = None ; error=Some 11; error_msg =  None} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*     NM Floating IPs *)
  let eval_create_floating_ip self (props:Apero.properties) =
    ignore props;
    Logs.debug (fun m -> m "[eval_create_floating_ip] - ##############");
    Logs.debug (fun m -> m "[eval_create_floating_ip] - Properties: %s" (Apero.Properties.to_string props));
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    Logs.debug (fun m -> m "[eval_create_floating_ip] - # NetManager: %s" net_p);
    try%lwt
      let fname = "create_floating_ip" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname [] state.yaks
      >>= fun res ->
      match res with
      | Some r ->
        Logs.debug (fun m -> m "[eval_create_floating_ip] - Eval Result %s" (FAgentTypes.string_of_eval_result r));
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
      Logs.err (fun m -> m "[eval_create_floating_ip] - # Error when creating floating IP:  %s" (Printexc.to_string exn));
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_delete_floating_ip self (props:Apero.properties) =
    Logs.debug (fun m -> m "[eval_delete_floating_ip] - ##############");
    Logs.debug (fun m -> m "[eval_delete_floating_ip] - Properties: %s" (Apero.Properties.to_string props));
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    Logs.debug (fun m -> m "[eval_delete_floating_ip- # NetManager: %s" net_p);
    try%lwt
      let ip_id = Apero.Option.get @@ Apero.Properties.get "floating_uuid" props in
      let parameters = [("ip_id",ip_id)] in
      let fname = "delete_floating_ip" in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
        Some r ->
        Logs.debug (fun m -> m "[eval_delete_floating_ip] - Eval Result %s" (FAgentTypes.string_of_eval_result r));
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
      Logs.err (fun m -> m "[eval_delete_floating_ip]- Error: %s %s" msg stack);
      let eval_res = FAgentTypes.{result = None ; error = Some 22; error_msg = Some (Printexc.to_string e)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_assign_floating_ip self (props:Apero.properties) =
    Logs.debug (fun m -> m "[eval_assign_floating_ip] - ##############");
    Logs.debug (fun m -> m "[eval_assign_floating_ip] - Properties: %s" (Apero.Properties.to_string props));
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
      Logs.err (fun m -> m "[eval_assign_floating_ip] - Exception: %s" (Printexc.to_string exn));
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_remove_floating_ip self (props:Apero.properties) =
    Logs.debug (fun m -> m "[eval_remove_floating_ip] - ##############");
    Logs.debug (fun m -> m "[eval_remove_floating_ip] - Properties: %s" (Apero.Properties.to_string props));
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
      Logs.err (fun m -> m "[eval_remove_floating_ip] - Exception: %s" (Printexc.to_string exn));
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  let eval_add_router_port self (props:Apero.properties) =
    ignore props;
    Logs.debug (fun m -> m "[eval_add_router_port] - ##############");
    Logs.debug (fun m -> m "[eval_add_router_port] - Properties: %s" (Apero.Properties.to_string props));
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    Logs.debug (fun m -> m "[eval_add_router_port] - # NetManager: %s" net_p);
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
        Logs.debug (fun m -> m "[eval_add_router_port] - Eval Result: %s" (FAgentTypes.string_of_eval_result r));
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
      Logs.err (fun m -> m "[eval_add_router_port] Exception: %s" (Printexc.to_string exn));
      let eval_res = FAgentTypes.{result = None ; error = Some 22; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res

  (*  *)
  let eval_remove_router_port self (props:Apero.properties) =
    ignore props;
    Logs.debug (fun m -> m "[eval_remove_router_port]- ##############");
    Logs.debug (fun m -> m "[eval_remove_router_port] - Properties: %s" (Apero.Properties.to_string props));
    MVar.read self >>= fun state ->
    let%lwt net_p = get_network_plugin self in
    Logs.debug (fun m -> m "[eval_remove_router_port] - # NetManager: %s" net_p);
    try%lwt
      let fname = "remove_router_port" in
      let rid = Apero.Option.get @@ Apero.Properties.get "router_id" props in
      let vid = Apero.Option.get @@ Apero.Properties.get "vnet_id" props in
      let parameters = [("router_id", rid); ("vnet_id", vid)] in
      Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get state.configuration.agent.uuid) net_p fname parameters state.yaks
      >>= fun res ->
      match res with
      | Some r ->
        Logs.debug (fun m -> m "[eval_remove_router_port] Eval Result: %s" (FAgentTypes.string_of_eval_result r));
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
      Logs.err (fun m -> m "[eval_remove_router_port] Exception: %s" (Printexc.to_string exn));
      let eval_res = FAgentTypes.{result = None ; error = Some 22; error_msg = Some (Printexc.to_string exn)} in
      Lwt.return @@ FAgentTypes.string_of_eval_result eval_res
  (*  *)
  let eval_heartbeat myuuid self (props:Apero.properties) =
    Logs.debug (fun m -> m "[eval_heartbeat]- ##############");
    Logs.debug (fun m -> m "[eval_heartbeat] - Properties: %s" (Apero.Properties.to_string props));
    let  _ = MVar.guarded self (fun state ->
    let source_id = Apero.Option.get @@ Apero.Properties.get "node_id" props in
    let timestamp = Unix.gettimeofday () in
    let current_available = state.available_nodes in
    let new_available =
      match List.find_opt (fun (n,_) -> String.compare n source_id == 0 ) current_available with
      | Some _ ->
            Logs.debug (fun m -> m "[eval_heartbeat] - Updating heartbeat information for %s" source_id);
            List.append (List.filter (fun (n,_) -> String.compare n source_id != 0) current_available) [(source_id,timestamp)]
      | None ->
        Logs.debug (fun m -> m "[eval_heartbeat] - Adding heartbeat information for %s" source_id);
        List.append current_available [(source_id,timestamp)]
    in
    let state = {state with available_nodes = new_available} in
    MVar.return () state)
    in
    let result = FTypes.{nodeid = myuuid } in
    let eval_res = FAgentTypes.{result = Some (JSON.of_string (FTypes.string_of_heartbeat_info result)) ; error = None; error_msg = None} in
    Logs.debug (fun m -> m "[eval_heartbeat] - Returning: %s" (FAgentTypes.string_of_eval_result eval_res));
    Lwt.return @@  FAgentTypes.string_of_eval_result eval_res
  (*  *)
  let eval_run_fdu myuuid instanceid self _ =
    Logs.debug (fun m -> m "[eval_run_fdu]- ##############");
    Logs.debug (fun m -> m "[eval_run_fdu]- InstanceID : %s" instanceid);
    MVar.read self >>= fun state ->
    let%lwt res = Yaks_connector.Local.Actual.run_fdu_in_node myuuid instanceid state.yaks in
    Lwt.return @@  FAgentTypes.string_of_eval_result res
  (*  *)
  let eval_log_fdu myuuid instanceid self _ =
    Logs.debug (fun m -> m "[eval_log_fdu]- ##############");
    Logs.debug (fun m -> m "[eval_log_fdu]- InstanceID : %s" instanceid);
    MVar.read self >>= fun state ->
    let%lwt res = Yaks_connector.Local.Actual.log_fdu_in_node myuuid instanceid state.yaks in
    Lwt.return @@  FAgentTypes.string_of_eval_result res
  (*  *)
  let eval_ls_fdu myuuid instanceid self _ =
    Logs.debug (fun m -> m "[eval_ls_fdu]- ##############");
    Logs.debug (fun m -> m "[eval_ls_fdu]- InstanceID : %s" instanceid);
    MVar.read self >>= fun state ->
    let%lwt res = Yaks_connector.Local.Actual.log_fdu_in_node myuuid instanceid state.yaks in
    Lwt.return @@  FAgentTypes.string_of_eval_result res
    (*  *)
  let eval_file_fdu myuuid instanceid self filename =
    Logs.debug (fun m -> m "[eval_ls_fdu]- ##############");
    Logs.debug (fun m -> m "[eval_ls_fdu]- InstanceID : %s" instanceid);
    MVar.read self >>= fun state ->
    let%lwt res = Yaks_connector.Local.Actual.file_fdu_in_node myuuid instanceid filename state.yaks in
    Lwt.return @@  FAgentTypes.string_of_eval_result res
