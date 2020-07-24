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
open Utils

(* Listeners *)
  (* Global Desired *)
  let cb_gd_plugin self (pl:FTypes.plugin option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match pl with
       | Some pl ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_plugin] - ##############");
         Logs.debug (fun m -> m "[cb_gd_plugin] - Received plugin");
         Logs.debug (fun m -> m "[cb_gd_plugin] - Name: %s" pl.name);
         Logs.debug (fun m -> m "[cb_gd_plugin] -  Calling the spawner by writing this on local desired");
         Yaks_connector.Local.Desired.add_node_plugin (Apero.Option.get self.configuration.agent.uuid) pl self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some plid ->
         MVar.read self >>= fun self ->
         Yaks_connector.Local.Desired.remove_node_plugin (Apero.Option.get self.configuration.agent.uuid) plid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  let cb_gd_fdu self (fdu:User.Descriptors.FDU.descriptor option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match fdu with
       | Some fdu -> MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_fdu] - ##############");
         Logs.debug (fun m -> m "[cb_gd_fdu] - FDU Updated! Advertising on GA");
         let fdu =
           match fdu.uuid with
           | Some _ -> fdu
           | None  ->
             let fduid = Apero.Uuid.to_string @@ Apero.Uuid.make_from_alias fdu.id in
             {fdu with uuid = Some fduid}
         in
         Yaks_connector.Global.Actual.add_catalog_fdu_info (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get fdu.uuid) fdu self.yaks >>= Lwt.return
       | None -> Lwt.return_unit
      )
    | true ->
      (match uuid with
       | Some fduid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_catalog_fdu_info (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id fduid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  let cb_gd_image self (img:User.Descriptors.FDU.image option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match img with
       | Some img ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_image] - ##############");
         Logs.debug (fun m -> m "[cb_gd_image] - Image Updated! Advertising on GA");
         (match img.uuid with
          | Some id -> Yaks_connector.Global.Actual.add_image (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id id img self.yaks >>= Lwt.return
          | None -> Logs.debug (fun m -> m "[cb_gd_image] - Ignoring Image as UUID is missing!!"); Lwt.return_unit)
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some nodeid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_image (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nodeid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)


  let cb_gd_flavor self (flv:User.Descriptors.FDU.computational_requirements option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match flv with
       | Some flv ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_flavor] - ##############");
         Logs.debug (fun m -> m "[cb_gd_flavor] - Flavor Updated! Advertising on GA");
         (match flv.uuid with
          | Some id -> Yaks_connector.Global.Actual.add_flavor (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id id flv self.yaks >>= Lwt.return
          | None -> Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FLAVOR - Ignoring Flavor as UUID is missing!!"); Lwt.return_unit)
       | None -> Lwt.return_unit
      )
    | true ->
      (match uuid with
       | Some flvid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_flavor (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id flvid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  let cb_gd_node_fdu self (fdu:Infra.Descriptors.FDU.record option) (is_remove:bool) (fduid:string option) (instanceid:string option) =
    match is_remove with
    | false ->
      (match fdu with
       | Some fdu ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_node_fdu] - ##############");
         Logs.debug (fun m -> m "[cb_gd_node_fdu] - FDU Updated! Agent will call the right plugin!");
         let%lwt fdu_d = Yaks_connector.Global.Actual.get_catalog_fdu_info (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id fdu.fdu_id self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
         let fdu_type = Fos_sdk.string_of_hv_type fdu_d.hypervisor in
         Logs.debug (fun m -> m "[cb_gd_node_fdu] - FDU Type %s" fdu_type);
         let%lwt plugins = Yaks_connector.Local.Actual.get_node_plugins (Apero.Option.get self.configuration.agent.uuid) self.yaks in
         let%lwt matching_plugins = Lwt_list.filter_map_p (fun e ->
             let%lwt pl = Yaks_connector.Local.Actual.get_node_plugin (Apero.Option.get self.configuration.agent.uuid) e self.yaks in
             if String.uppercase_ascii (pl.name) = String.uppercase_ascii (fdu_type) then
               Lwt.return @@ Some pl
             else
               Lwt.return None
           ) plugins
         in
         (match matching_plugins with
          | [] ->
            Logs.err (fun m -> m "[cb_gd_node_fdu] - No plugin found for this FDU");
            let r = { fdu with status = `ERROR} in
            Yaks_connector.Global.Actual.add_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) r.fdu_id r.uuid r self.yaks >>= Lwt.return
          | _ ->
            let pl = List.hd matching_plugins in
            Logs.debug (fun m -> m "[cb_gd_node_fdu]- Calling %s plugin" pl.name);
            (* Here we should at least update the record interfaces and connection points *)
            match fdu.status with
            | `DEFINE ->
              Logs.err (fun m -> m "[cb_gd_node_fdu] - FDU Define it is not supposed to pass by this function");
              Lwt.return_unit
            | _ -> Yaks_connector.Local.Desired.add_node_fdu (Apero.Option.get self.configuration.agent.uuid) pl.uuid fdu.fdu_id fdu.uuid fdu self.yaks >>= Lwt.return
         )

       | None -> Lwt.return_unit)
    | true ->
      (match( fduid,instanceid)  with
       | Some fduid , Some instanceid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fduid instanceid self.yaks >>= Lwt.return
       | (_,_) -> Lwt.return_unit)


  (*  *)
  let cb_gd_net_all self (net:FTypes.virtual_network option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin_info self in
    (* let face =
      match net_p.configuration with
      | Some c ->
        Yojson.Safe.to_string @@ Yojson.Safe.Util.member "dataplane_interface" c
      | None ->
          Logs.debug (fun m -> m "[cb_gd_net_all] - Missing data plane interface configuration");
          ""
    in *)
    match is_remove with
    | false ->
      (match net with
       | Some net ->
         MVar.read self >>= fun self ->

         Logs.debug (fun m -> m "[cb_gd_net_all] - ##############");
         Logs.debug (fun m -> m "[cb_gd_net_all] - vNET Updated! Agent will update actual store");
         Logs.debug (fun m -> m "[cb_gd_net_all] - vNET Info: %s" (FTypes.string_of_virtual_network net));
         let%lwt nd = Yaks_connector.Global.Actual.get_network (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id net.uuid self.yaks in
         let net_exists =
          (match nd with
          | Some _ -> true
          | None -> false)
          in
         (* let net = match net.face with | Some _ -> net | None -> {net with face = Some face } in *)
         (* let net = match net.br_name with | Some _ -> net | None -> {net with br_name = Some (Printf.sprintf "fos-br-%d" vni)} in *)
         (match net_exists with
         | true ->
          Logs.debug (fun m -> m "[cb_gd_net_all] - vNET already existing: %s"  net.uuid);
          Lwt.return_unit
         | false ->
          let vni = (Random.int 16777215) in
          let net = match net.vni with | Some _ -> net | None -> {net with vni = Some vni } in
          let net = match net.mcast_addr with | Some _ -> net | None -> {net with mcast_addr = Some (Printf.sprintf "239.0.%d.%d" (Random.int 255) (Random.int 255))} in
          let net = match net.port with | Some _ -> net | None -> {net with port = Some 4789} in
          let net = match net.overlay with | Some _ -> net | None -> {net with overlay = Some true} in
          let net = match net.vlan_id with | Some _ -> net | None -> {net with vlan_id = Some (Random.int 4095) } in
          Logs.debug (fun m -> m "[cb_gd_net_all] - vNET Info Updated: %s" (FTypes.string_of_virtual_network net));
          let%lwt _ = Yaks_connector.Global.Actual.add_network (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id net.uuid net self.yaks in
          Lwt.return_unit)
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some netid -> MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_net_all] - ##############");
         Logs.debug (fun m -> m "[cb_gd_net_all] - vNET Removed!");
         let%lwt net_info = Yaks_connector.Local.Actual.get_node_network (Apero.Option.get self.configuration.agent.uuid) net_p.uuid netid self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
         let net_info = {net_info with status = `DESTROY} in
         let%lwt _ = Yaks_connector.Local.Desired.add_node_network (Apero.Option.get self.configuration.agent.uuid) net_p.uuid netid net_info self.yaks in
         Yaks_connector.Global.Actual.remove_network (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id netid self.yaks >>= Lwt.return
       | None ->
         Logs.debug (fun m -> m "[cb_gd_net_all] - vNET NO UUID!!!!");
         Lwt.return_unit)


  (*  *)
  let cb_gd_net self (net:FTypesRecord.virtual_network option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      (match net with
       | Some net ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_net] - ##############");
         Logs.debug (fun m -> m "[cb_gd_net] - vNET Updated! Agent will update actual store and call the right plugin!");
         (* let%lwt _ = Yaks_connector.Global.Actual.add_network (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id net.uuid net self.yaks in *)
         (* let record = FTypesRecord.{uuid = net.uuid; status = `CREATE; properties = None; ip_configuration = net.ip_configuration} in *)
         Yaks_connector.Local.Desired.add_node_network (Apero.Option.get self.configuration.agent.uuid) net_p net.uuid net self.yaks
         >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some netid -> MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_net] - ##############");
         Logs.debug (fun m -> m "[cb_gd_net]- vNET Removed!");
         let%lwt net_info = Yaks_connector.Local.Actual.get_node_network (Apero.Option.get self.configuration.agent.uuid) net_p netid self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
         let net_info = {net_info with status = `DESTROY} in
         let%lwt _ = Yaks_connector.Local.Desired.add_node_network (Apero.Option.get self.configuration.agent.uuid) net_p netid net_info self.yaks in
         Yaks_connector.Global.Actual.remove_network (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id netid self.yaks >>= Lwt.return
       | None ->
         Logs.debug (fun m -> m "[cb_gd_net] - vNET Missing UUID!!!!");
         Lwt.return_unit)


  let cb_gd_cp self (cp:User.Descriptors.Network.connection_point_descriptor option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      ( match cp with
        | Some cp ->
          MVar.read self >>= fun self ->
          Logs.debug (fun m -> m "[cb_gd_cp] - ##############");
          Logs.debug (fun m -> m "[cb_gd_cp] CP Updated! Agent will update actual store and call the right plugin!");
          let%lwt _ = Yaks_connector.Global.Actual.add_port (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id cp.id cp self.yaks in
          let record = Infra.Descriptors.Network.{cp_id = cp.id; uuid = cp.id; status = `CREATE; properties = None; veth_face_name = None; br_name = None;cp_type= Some `VPORT; port_security_enabled=None; vld_ref = cp.vld_ref} in
          Yaks_connector.Local.Desired.add_node_port (Apero.Option.get self.configuration.agent.uuid) net_p record.uuid record self.yaks
          >>= Lwt.return
        | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some cpid -> MVar.read self >>= fun self ->
         let%lwt _ = Yaks_connector.Global.Actual.remove_node_port (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) cpid self.yaks in
         Yaks_connector.Global.Actual.remove_port (Apero.Option.get @@ self.configuration.agent.system) (Apero.Option.get @@ self.configuration.agent.system) cpid self.yaks  >>= Lwt.return
       | None -> Lwt.return_unit)


  let cb_gd_router self (router:Router.descriptor option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      (match router with
       | Some router ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_router] - ##############");
         Logs.debug (fun m -> m "[cb_gd_router] - vRouter Updated! Agent will update actual store and call the right plugin!");
         (* Converting to record *)
         let rid = (Apero.Option.get_or_default router.uuid (Apero.Uuid.to_string (Apero.Uuid.make ()))) in
         let%lwt port_records = Lwt_list.mapi_p (fun i (e:Router.router_port) ->
             match e.port_type with
             | `EXTERNAL ->
               let%lwt res = Yaks_connector.Local.Actual.exec_nm_eval (Apero.Option.get self.configuration.agent.uuid) net_p "get_overlay_interface" [] self.yaks  in
               let face =(JSON.to_string (Apero.Option.get (Apero.Option.get res).result)) in
               (* This is a bad example of removing the escape characters, the JSON.string  *)
               let face = String.sub face 1 ((String.length face)-2) in
               let wan_face = Printf.sprintf "r-%s-e%d" (List.hd (String.split_on_char '-' rid)) i in
               Lwt.return Router.{port_type = `EXTERNAL; faces = [wan_face]; ext_face = Some face; ip_address = ""; vnet_id = None}
             | `INTERNAL ->
               let face_i = Printf.sprintf "r-%s-e%d-i" (List.hd (String.split_on_char '-' rid)) i in
               let face_e = Printf.sprintf "r-%s-e%d-e" (List.hd (String.split_on_char '-' rid)) i in
               Lwt.return Router.{port_type = `INTERNAL; faces = [face_i; face_e]; ip_address = Apero.Option.get_or_default e.ip_address ""; ext_face = None; vnet_id = e.vnet_id}
           ) router.ports
         in
         let vrouter_ns =  Printf.sprintf "r-%s-ns" (List.hd (String.split_on_char '-' rid))  in
         let router_record = Router.{uuid = rid; state = `CREATE; ports = port_records; router_ns = vrouter_ns; nodeid = (Apero.Option.get self.configuration.agent.uuid)} in
         Yaks_connector.Local.Desired.add_node_router (Apero.Option.get self.configuration.agent.uuid) net_p  router_record.uuid router_record self.yaks
         >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some routerid -> MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_gd_router] - ##############");
         Logs.debug (fun m -> m "[cb_gd_router] vRouter Removed!");
         let%lwt router_info = Yaks_connector.Local.Actual.get_node_router (Apero.Option.get self.configuration.agent.uuid) net_p routerid self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
         let router_info = {router_info with state = `DESTROY} in
         Yaks_connector.Local.Desired.add_node_router (Apero.Option.get self.configuration.agent.uuid) net_p routerid router_info self.yaks
       (* Yaks_connector.Global.Actual.reove_ (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id routerid self.yaks >>= Lwt.return *)
       | None ->
         Logs.debug (fun m -> m "[cb_gd_router] - vRouter Missing UUID!!!!");
         Lwt.return_unit)


  (* Local Actual *)
  let cb_la_net self (net:FTypesRecord.virtual_network option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      (match net with
       | Some net ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_la_net] - ##############");
         Logs.debug (fun m -> m "[cb_la_net] - vNET Updated! Advertising on GA");
         let nid = (Apero.Option.get self.configuration.agent.uuid) in
         (match net.status with
          | `DESTROY -> Yaks_connector.Global.Actual.remove_node_network (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nid net.uuid self.yaks
          | _ ->
            Yaks_connector.Global.Actual.add_node_network (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nid net.uuid net self.yaks >>= Lwt.return)
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some netid ->
         MVar.read self >>= fun self ->
         Yaks_connector.Local.Desired.remove_node_network (Apero.Option.get self.configuration.agent.uuid) net_p netid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  let cb_la_cp self (cp:Infra.Descriptors.FDU.connection_point_record option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      (match cp with
       | Some cp ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_la_cp] - ##############");
         Logs.debug (fun m -> m "[cb_la_cp] - CP Updated! Advertising on GA");
         let nid = (Apero.Option.get self.configuration.agent.uuid) in
         ( match cp.status with
           | `DESTROY -> Yaks_connector.Global.Actual.remove_node_port (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nid cp.cp_id self.yaks
           | _ ->
             Yaks_connector.Global.Actual.add_node_port (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nid cp.cp_id cp self.yaks >>= Lwt.return)
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some cpid ->
         MVar.read self >>= fun self ->
         Yaks_connector.Local.Desired.remove_node_port (Apero.Option.get self.configuration.agent.uuid) net_p cpid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)


  let cb_la_router self (router:Router.record option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      (match router with
       | Some router ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_la_router] - ##############");
         Logs.debug (fun m -> m "[cb_la_router] - vRouter Updated! Advertising on GA");
         let nid = (Apero.Option.get self.configuration.agent.uuid) in
         (match router.state with
          | `DESTROY -> Yaks_connector.Global.Actual.remove_node_router (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nid router.uuid self.yaks
          | _ ->
            (* Convert to back to descriptor *)
            let%lwt ports = Lwt_list.map_p (fun (e:Router.router_port_record) ->
                Lwt.return Router.{port_type = e.port_type; vnet_id = e.vnet_id; ip_address = Some e.ip_address}
              ) router.ports
            in
            let router_desc = Router.{uuid = Some router.uuid; ports = ports; } in
            Yaks_connector.Global.Actual.add_node_router (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nid router.uuid router_desc self.yaks >>= Lwt.return)
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some routerid ->
         MVar.read self >>= fun self ->
         Yaks_connector.Local.Desired.remove_node_router (Apero.Option.get self.configuration.agent.uuid) net_p routerid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  let cb_la_plugin self (pl:FTypes.plugin option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match pl with
       | Some pl ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_la_plugin] - ##############");
         Logs.debug (fun m -> m "[cb_la_plugin] - Received plugin");
         Logs.debug (fun m -> m "[cb_la_plugin] - Name: %s" pl.name);
         Logs.debug (fun m -> m "[cb_la_plugin] -  Plugin loaded advertising on GA");
         Yaks_connector.Global.Actual.add_node_plugin (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) pl self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some plid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_plugin  (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) plid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)


  let cb_la_ni self (ni:FTypes.node_info option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match ni with
       | Some ni ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_la_ni] - ##############");
         Logs.debug (fun m -> m "[cb_la_ni] - Updated node info advertising of GA");
         Yaks_connector.Global.Actual.add_node_info (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) ni self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some nid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_info (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)


  let cb_la_ns self (ns:FTypes.node_status option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match ns with
       | Some ns ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_la_ns] ##############");
         Logs.debug (fun m -> m "[cb_la_ns] - Updated node info advertising of GA");
         Yaks_connector.Global.Actual.add_node_status (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) ns self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some nid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_status (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  let cb_la_node_fdu state (fdu:Infra.Descriptors.FDU.record option) (is_remove:bool) (fduid:string option) (instanceid:string option) =
    match is_remove with
    | false ->
      (match fdu with
       | Some fdu ->
         MVar.read state >>= fun self ->
         Logs.debug (fun m -> m "[cb_la_node_fdu] - ##############");
         Logs.debug (fun m -> m "[cb_la_node_fdu]- FDU Updated! Advertising on GA");
         ( match fdu.status with
           | `UNDEFINE -> Yaks_connector.Global.Actual.remove_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid self.yaks
           | `CONFIGURE ->
            let start_f = (Evals.eval_start_fdu (Apero.Option.get self.configuration.agent.uuid) fdu.uuid state) in
            let run_f = (Evals.eval_run_fdu (Apero.Option.get self.configuration.agent.uuid) fdu.uuid state) in
            let ls_f = (Evals.eval_ls_fdu (Apero.Option.get self.configuration.agent.uuid) fdu.uuid state) in
            let log_f = (Evals.eval_log_fdu (Apero.Option.get self.configuration.agent.uuid) fdu.uuid state) in
            let file_f = (Evals.eval_file_fdu (Apero.Option.get self.configuration.agent.uuid) fdu.uuid state) in
            let _ = Yaks_connector.Global.Actual.add_fdu_start_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid start_f self.yaks in
            let _ = Yaks_connector.Global.Actual.add_fdu_run_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid run_f self.yaks in
            let _ = Yaks_connector.Global.Actual.add_fdu_ls_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid ls_f self.yaks in
            let _ = Yaks_connector.Global.Actual.add_fdu_log_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid log_f self.yaks in
            let _ = Yaks_connector.Global.Actual.add_fdu_file_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid file_f self.yaks in
            Yaks_connector.Global.Actual.add_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid fdu self.yaks >>= Lwt.return
           | `CLEAN ->
            let _ = Yaks_connector.Global.Actual.remove_fdu_start_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid self.yaks in
            let _ = Yaks_connector.Global.Actual.remove_fdu_run_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid self.yaks in
            let _ = Yaks_connector.Global.Actual.remove_fdu_log_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid self.yaks in
            let _ = Yaks_connector.Global.Actual.remove_fdu_ls_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid self.yaks in
            let _ = Yaks_connector.Global.Actual.remove_fdu_file_eval (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid self.yaks in
            Yaks_connector.Global.Actual.add_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid fdu self.yaks >>= Lwt.return
           | _ ->
             Yaks_connector.Global.Actual.add_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid fdu self.yaks >>= Lwt.return)
       | None -> Lwt.return_unit)
    | true ->
      (match( fduid,instanceid)  with
       | Some fduid , Some instanceid -> MVar.read state >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fduid instanceid self.yaks >>= Lwt.return
       | (_,_) -> Lwt.return_unit)



  (* Constrained Nodes Global *)
  (* let cb_gd_cnode_fdu self nodeid (fdu:FDU.record option) (is_remove:bool) (uuid:string option) =
     match is_remove with
     | false ->
     (match fdu with
     | Some fdu ->
     MVar.read self >>= fun self ->
     Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-CNODE-FDU - ##############") in
     Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-CNODE-FDU - FDU Updated! Agent will call the right plugin!") in
     let%lwt fdu_d = Yaks_connector.Global.Actual.get_fdu_info (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id fdu.fdu_uuid self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
     let fdu_type = Fos_sdk.string_of_hv_type fdu_d.hypervisor in
     Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-CNODE-FDU - FDU Type %s" fdu_type) in
     let%lwt plugins = Yaks_connector.LocalConstraint.Actual.get_node_plugins nodeid self.yaks in
     Lwt_list.iter_p (fun e ->
     let%lwt pl = Yaks_connector.LocalConstraint.Actual.get_node_plugin nodeid e self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
     if String.uppercase_ascii (pl.name) = String.uppercase_ascii (fdu_type) then
     Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FDU - Calling %s plugin" pl.name) in
     Yaks_connector.LocalConstraint.Desired.add_node_fdu nodeid pl.uuid fdu.fdu_uuid fdu self.yaks >>= Lwt.return
     else
     Lwt.return_unit
     ) plugins >>= Lwt.return
     | None -> Lwt.return_unit)
     | true ->
     (match uuid with
     | Some fduid -> MVar.read self >>= fun self ->
     Lwt.return_unit
     (* Yaks_connector.Global.Desired.remove_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nodeid fduid self.yaks >>= Lwt.return *)
     | None -> Lwt.return_unit)
     in *)
  (* Constrained Nodes Local *)
  let cb_lac_node_fdu self _ (fdu:Infra.Descriptors.FDU.record option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match fdu with
       | Some fdu ->
         MVar.read self >>= fun _ ->
         Logs.debug (fun m -> m "[cb_lac_node_fdu] - ##############");
         Logs.debug (fun m -> m "[cb_lac_node_fdu] - FDU Updated! Advertising on GA");
         ( match fdu.status with
           | `UNDEFINE -> Lwt.return_unit
           (* Yaks_connector.Global.Actual.remove_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nodeid fdu.fdu_uuid self.yaks *)
           | _ ->Lwt.return_unit)
       (* Yaks_connector.Global.Actual.add_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nodeid fdu.fdu_uuid fdu self.yaks >>= Lwt.return) *)
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some _ -> MVar.read self >>= fun _ -> Lwt.return_unit
       (* Yaks_connector.Global.Actual.remove_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nodeid fduid self.yaks >>= Lwt.return *)
       | None -> Lwt.return_unit)

  let cb_lac_plugin self nodeid (pl:FTypes.plugin option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match pl with
       | Some pl ->
         MVar.read self >>= fun self ->
         Logs.debug (fun m -> m "[cb_lac_plugin] - ##############");
         Logs.debug (fun m -> m "[cb_lac_plugin] - Received plugin");
         Logs.debug (fun m -> m "[cb_lac_plugin] - Name: %s" pl.name);
         Logs.debug (fun m -> m "[cb_lac_plugin] -  Plugin loaded advertising on GA");
         Yaks_connector.Global.Actual.add_node_plugin (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nodeid pl self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some plid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_plugin (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nodeid plid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  (* let cb_lac_node_configuration self nodeid (pl:FTypes.node_info) =
     MVar.read self >>= fun self ->
     Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-CONF - ##############") in
     Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-CONF - Received plugin") in
     Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-CONF - Name: %s" pl.name) in
     Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-CONF -  Plugin loaded advertising on GA") in
     Yaks_connector.Global.Actual.add_node_configuration (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id nodeid pl self.yaks >>= Lwt.return
     in *)
  let cb_lac_nodes self (ni:FTypes.node_info option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match ni with
       | Some ni ->
         MVar.guarded self @@ fun state ->
         Logs.debug (fun m -> m "[cb_lac_nodes] - ##############");
         Logs.debug (fun m -> m "[cb_lac_nodes] - Updated node info advertising on GA");
         let intid = ni.uuid in
         let extid = Apero.Uuid.to_string @@ Apero.Uuid.make () in
         let ext_ni = {ni with uuid = extid } in
         let%lwt _ = Yaks_connector.Global.Actual.add_node_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id extid ext_ni state.yaks in
         let%lwt _ = Yaks_connector.LocalConstraint.Actual.observe_node_plugins intid (cb_lac_plugin self  extid) state.yaks in
         let%lwt _ =  Yaks_connector.LocalConstraint.Actual.observe_node_fdu intid (cb_lac_node_fdu self extid) state.yaks in
         (* let%lwt _ = Yaks_connector.Global.Desired.observe_node_fdu (Apero.Option.get @@ self.configuration.agent.system) Yaks_connector.default_tenant_id extid (cb_gd_cnode_fdu self intid) state.yaks in *)
         MVar.return () {state with constrained_nodes = ConstraintMap.add extid intid state.constrained_nodes}
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some nid -> MVar.guarded self @@ fun state ->
         let%lwt _ = Yaks_connector.Global.Actual.remove_node_info (Apero.Option.get @@ state.configuration.agent.system) Yaks_connector.default_tenant_id nid state.yaks in
         MVar.return () {state with constrained_nodes =  ConstraintMap.remove nid state.constrained_nodes}
       | None -> Lwt.return_unit)

  (* let cb_ga_nodes self (ni:FTypes.node_info option) (is_remove:bool) (uuid:string option) =
    try%lwt
      (match is_remove with
      | false ->
        (match ni with
        | Some node_info ->
          MVar.guarded self (fun state ->
          let nid = node_info.uuid in
          match String.compare nid (Apero.Option.get state.configuration.agent.uuid) with
          | 0 -> MVar.return () state
          | _ ->
          Logs.debug (fun m -> m "[cb_ga_nodes] - ##############");
          Logs.debug (fun m -> m "[cb_ga_nodes] - Received Node Advertisement - %s" nid);
          (* let ping_task_starter = Utils.start_ping_task (Apero.Option.get state.configuration.agent.uuid) state.yaks nid in *)
          let open FTypes in
          let faces_info = List.filter (fun fi -> (Utils.filter_face fi.intf_name)) node_info.network in
          (* extract address information *)
          let ip_infos = List.map (fun fi -> (fi.intf_configuration.ipv4_address,fi.intf_configuration.ipv6_address)) faces_info in
          let%lwt hb_info = Lwt_list.map_p
              (fun (ip,_ ) ->
                let blen = 1024 in
                let rbuf = Bytes.create blen in
                let task = MVar.create Heartbeat.{peer=nid; timeout=2.0; peer_address=ip; rbuf=rbuf; blen=blen; avg=0.0; packet_sent=0; packet_received=0; bytes_sent=0; bytes_received=0; run=true} in
                Logs.debug (fun m -> m "[cb_ga_nodes] - Spawing task - %s - %s" nid ip);
                let _ = Utils.start_ping_task (Apero.Option.get state.configuration.agent.uuid) state.yaks nid ip task in
                Lwt.return task
              ) ip_infos
          in
          let rec add_task_inner tasks current_tasks =
          match tasks with
          | hd::tl ->
                  let new_tasks = List.append current_tasks [hd] in
                  add_task_inner tl new_tasks
          | [] -> current_tasks
        in
        let new_tasks = add_task_inner hb_info [] in
        let new_map = Heartbeat.HeartbeatMap.add nid new_tasks state.ping_tasks in
        let state = {state with ping_tasks = new_map } in
        Logs.debug (fun m -> m "[cb_ga_nodes] - Started ping of %s" nid);
        MVar.return () state)
        | None -> Lwt.return_unit)
      | true ->
        (match uuid with
        | Some nid -> MVar.guarded self (fun state ->
            match Heartbeat.HeartbeatMap.mem nid state.ping_tasks with
            | false ->  MVar.return () state
            | true ->
              Logs.debug (fun m -> m "[cb_ga_nodes] Node disappeared - %s" nid);
              let tasks = Heartbeat.HeartbeatMap.find nid state.ping_tasks in
              let state  = {state with ping_tasks =  Heartbeat.HeartbeatMap.remove nid  state.ping_tasks } in
              let%lwt _ = Lwt_list.iter_p (fun task -> MVar.guarded task (fun (t : Heartbeat.statistics) ->   MVar.return () {t with run = false})) tasks in
              Logs.debug (fun m -> m "[cb_ga_nodes]- Stopping ping tasks for - %s" nid);
              MVar.return () state)
        | None -> Lwt.return_unit))
    with
    | exn ->
      Logs.err (fun m -> m "[cb_ga_nodes] Exception %s raised:\n%s" (Printexc.to_string exn) (Printexc.get_backtrace ()));
      Lwt.return_unit *)