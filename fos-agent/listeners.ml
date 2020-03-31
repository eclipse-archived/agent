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
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-PLUGIN - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-PLUGIN - Received plugin") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-PLUGIN - Name: %s" pl.name) in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-PLUGIN -  Calling the spawner by writing this on local desired") in
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
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FDU - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FDU - FDU Updated! Advertising on GA") in
         let fdu =
           match fdu.uuid with
           | Some _ -> fdu
           | None  ->
             let fduid = Apero.Uuid.to_string @@ Apero.Uuid.make_from_alias fdu.id in
             {fdu with uuid = Some fduid}
         in
         Yaks_connector.Global.Actual.add_catalog_fdu_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get fdu.uuid) fdu self.yaks >>= Lwt.return
       | None -> Lwt.return_unit
      )
    | true ->
      (match uuid with
       | Some fduid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_catalog_fdu_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id fduid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  let cb_gd_image self (img:User.Descriptors.FDU.image option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match img with
       | Some img ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-IMAGE - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-IMAGE - Image Updated! Advertising on GA") in
         (match img.uuid with
          | Some id -> Yaks_connector.Global.Actual.add_image Yaks_connector.default_system_id Yaks_connector.default_tenant_id id img self.yaks >>= Lwt.return
          | None -> Lwt.return @@ Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-IMAGE - Ignoring Image as UUID is missing!!") >>= Lwt.return)
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some nodeid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_image Yaks_connector.default_system_id Yaks_connector.default_tenant_id nodeid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)


  let cb_gd_flavor self (flv:User.Descriptors.FDU.computational_requirements option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match flv with
       | Some flv ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FLAVOR - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FLAVOR - Flavor Updated! Advertising on GA") in
         (match flv.uuid with
          | Some id -> Yaks_connector.Global.Actual.add_flavor Yaks_connector.default_system_id Yaks_connector.default_tenant_id id flv self.yaks >>= Lwt.return
          | None -> Lwt.return @@ Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FLAVOR - Ignoring Flavor as UUID is missing!!") >>= Lwt.return)
       | None -> Lwt.return_unit
      )
    | true ->
      (match uuid with
       | Some flvid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_flavor Yaks_connector.default_system_id Yaks_connector.default_tenant_id flvid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  let cb_gd_node_fdu self (fdu:Infra.Descriptors.FDU.record option) (is_remove:bool) (fduid:string option) (instanceid:string option) =
    match is_remove with
    | false ->
      (match fdu with
       | Some fdu ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NODE-FDU - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NODE-FDU - FDU Updated! Agent will call the right plugin!") in
         let%lwt fdu_d = Yaks_connector.Global.Actual.get_catalog_fdu_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id fdu.fdu_id self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
         let fdu_type = Fos_sdk.string_of_hv_type fdu_d.hypervisor in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FDU - FDU Type %s" fdu_type) in
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
            let _ = Logs.err (fun m -> m "[FOS-AGENT] - CB-GD-FDU - No plugin found for this FDU") in
            let r = { fdu with status = `ERROR} in
            Yaks_connector.Global.Actual.add_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) r.fdu_id r.uuid r self.yaks >>= Lwt.return
          | _ ->
            let pl = List.hd matching_plugins in
            let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FDU - Calling %s plugin" pl.name) in
            (* Here we should at least update the record interfaces and connection points *)
            match fdu.status with
            | `DEFINE ->
              let _ = Logs.err (fun m -> m "[FOS-AGENT] - CB-GD-FDU - FDU Define it is not supposed to pass by this function") in
              Lwt.return_unit
            | _ -> Yaks_connector.Local.Desired.add_node_fdu (Apero.Option.get self.configuration.agent.uuid) pl.uuid fdu.fdu_id fdu.uuid fdu self.yaks >>= Lwt.return
         )

       | None -> Lwt.return_unit)
    | true ->
      (match( fduid,instanceid)  with
       | Some fduid , Some instanceid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fduid instanceid self.yaks >>= Lwt.return
       | (_,_) -> Lwt.return_unit)


  (*  *)
  let cb_gd_net_all self (net:FTypes.virtual_network option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      (match net with
       | Some net ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - vNET Updated! Agent will update actual store and call the right plugin!") in
         let%lwt _ = Yaks_connector.Global.Actual.add_network Yaks_connector.default_system_id Yaks_connector.default_tenant_id net.uuid net self.yaks in
         (* let record = FTypesRecord.{uuid = net.uuid; status = `CREATE; properties = None; ip_configuration = net.ip_configuration; overlay = None; vni = None; mcast_addr = None; vlan_id = None; face = None} in *)
         (* Yaks_connector.Local.Desired.add_node_network (Apero.Option.get self.configuration.agent.uuid) net_p net.uuid record self.yaks *)
         Lwt.return_unit
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some netid -> MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - vNET Removed!") in
         let%lwt net_info = Yaks_connector.Local.Actual.get_node_network (Apero.Option.get self.configuration.agent.uuid) net_p netid self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
         let net_info = {net_info with status = `DESTROY} in
         let%lwt _ = Yaks_connector.Local.Desired.add_node_network (Apero.Option.get self.configuration.agent.uuid) net_p netid net_info self.yaks in
         Yaks_connector.Global.Actual.remove_network Yaks_connector.default_system_id Yaks_connector.default_tenant_id netid self.yaks >>= Lwt.return
       | None ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - vNET NO UUID!!!!") in
         Lwt.return_unit)


  (*  *)
  let cb_gd_net self (net:FTypesRecord.virtual_network option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      (match net with
       | Some net ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - vNET Updated! Agent will update actual store and call the right plugin!") in
         (* let%lwt _ = Yaks_connector.Global.Actual.add_network Yaks_connector.default_system_id Yaks_connector.default_tenant_id net.uuid net self.yaks in *)
         (* let record = FTypesRecord.{uuid = net.uuid; status = `CREATE; properties = None; ip_configuration = net.ip_configuration} in *)
         Yaks_connector.Local.Desired.add_node_network (Apero.Option.get self.configuration.agent.uuid) net_p net.uuid net self.yaks
         >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some netid -> MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - vNET Removed!") in
         let%lwt net_info = Yaks_connector.Local.Actual.get_node_network (Apero.Option.get self.configuration.agent.uuid) net_p netid self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
         let net_info = {net_info with status = `DESTROY} in
         let%lwt _ = Yaks_connector.Local.Desired.add_node_network (Apero.Option.get self.configuration.agent.uuid) net_p netid net_info self.yaks in
         Yaks_connector.Global.Actual.remove_network Yaks_connector.default_system_id Yaks_connector.default_tenant_id netid self.yaks >>= Lwt.return
       | None ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-NET - vNET NO UUID!!!!") in
         Lwt.return_unit)


  let cb_gd_cp self (cp:User.Descriptors.Network.connection_point_descriptor option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      ( match cp with
        | Some cp ->
          MVar.read self >>= fun self ->
          let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-CP - ##############") in
          let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-CP - CP Updated! Agent will update actual store and call the right plugin!") in
          let%lwt _ = Yaks_connector.Global.Actual.add_port Yaks_connector.default_system_id Yaks_connector.default_tenant_id cp.id cp self.yaks in
          let record = Infra.Descriptors.Network.{cp_id = cp.id; uuid = cp.id; status = `CREATE; properties = None; veth_face_name = None; br_name = None;cp_type= Some `VPORT; port_security_enabled=None; vld_ref = cp.vld_ref} in
          Yaks_connector.Local.Desired.add_node_port (Apero.Option.get self.configuration.agent.uuid) net_p record.uuid record self.yaks
          >>= Lwt.return
        | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some cpid -> MVar.read self >>= fun self ->
         let%lwt _ = Yaks_connector.Global.Actual.remove_node_port Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) cpid self.yaks in
         Yaks_connector.Global.Actual.remove_port Yaks_connector.default_system_id Yaks_connector.default_system_id cpid self.yaks  >>= Lwt.return
       | None -> Lwt.return_unit)


  let cb_gd_router self (router:Router.descriptor option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      (match router with
       | Some router ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-ROUTER - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-ROUTER - vRouter Updated! Agent will update actual store and call the right plugin!") in
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
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-ROUTER - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-ROUTER - vRouter Removed!") in
         let%lwt router_info = Yaks_connector.Local.Actual.get_node_router (Apero.Option.get self.configuration.agent.uuid) net_p routerid self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
         let router_info = {router_info with state = `DESTROY} in
         Yaks_connector.Local.Desired.add_node_router (Apero.Option.get self.configuration.agent.uuid) net_p routerid router_info self.yaks
       (* Yaks_connector.Global.Actual.reove_ Yaks_connector.default_system_id Yaks_connector.default_tenant_id routerid self.yaks >>= Lwt.return *)
       | None ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-ROUTER - vRouter NO UUID!!!!") in
         Lwt.return_unit)


  (* Local Actual *)
  let cb_la_net self (net:FTypesRecord.virtual_network option) (is_remove:bool) (uuid:string option) =
    let%lwt net_p = get_network_plugin self in
    match is_remove with
    | false ->
      (match net with
       | Some net ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-NET - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-NET - vNET Updated! Advertising on GA") in
         let nid = (Apero.Option.get self.configuration.agent.uuid) in
         (match net.status with
          | `DESTROY -> Yaks_connector.Global.Actual.remove_node_network Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid net.uuid self.yaks
          | _ ->
            Yaks_connector.Global.Actual.add_node_network Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid net.uuid net self.yaks >>= Lwt.return)
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
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-CP - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-CP - CP Updated! Advertising on GA") in
         let nid = (Apero.Option.get self.configuration.agent.uuid) in
         ( match cp.status with
           | `DESTROY -> Yaks_connector.Global.Actual.remove_node_port Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid cp.cp_id self.yaks
           | _ ->
             Yaks_connector.Global.Actual.add_node_port Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid cp.cp_id cp self.yaks >>= Lwt.return)
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
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-ROUTER - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-ROUTER - vRouter Updated! Advertising on GA") in
         let nid = (Apero.Option.get self.configuration.agent.uuid) in
         (match router.state with
          | `DESTROY -> Yaks_connector.Global.Actual.remove_node_router Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid router.uuid self.yaks
          | _ ->
            (* Convert to back to descriptor *)
            let%lwt ports = Lwt_list.map_p (fun (e:Router.router_port_record) ->
                Lwt.return Router.{port_type = e.port_type; vnet_id = e.vnet_id; ip_address = Some e.ip_address}
              ) router.ports
            in
            let router_desc = Router.{uuid = Some router.uuid; ports = ports; } in
            Yaks_connector.Global.Actual.add_node_router Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid router.uuid router_desc self.yaks >>= Lwt.return)
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
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-PLUGIN - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-PLUGIN - Received plugin") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-PLUGIN - Name: %s" pl.name) in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-PLUGIN -  Plugin loaded advertising on GA") in
         Yaks_connector.Global.Actual.add_node_plugin Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) pl self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some plid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_plugin  Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) plid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)


  let cb_la_ni self (ni:FTypes.node_info option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match ni with
       | Some ni ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-NI - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-NI - Updated node info advertising of GA") in
         Yaks_connector.Global.Actual.add_node_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) ni self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some nid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)


  let cb_la_ns self (ns:FTypes.node_status option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match ns with
       | Some ns ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-NI - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-NI - Updated node info advertising of GA") in
         Yaks_connector.Global.Actual.add_node_status Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) ns self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some nid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_status Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  let cb_la_node_fdu self (fdu:Infra.Descriptors.FDU.record option) (is_remove:bool) (fduid:string option) (instanceid:string option) =
    match is_remove with
    | false ->
      (match fdu with
       | Some fdu ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-NODE-FDU - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LA-NODE-FDU - FDU Updated! Advertising on GA") in
         ( match fdu.status with
           | `UNDEFINE -> Yaks_connector.Global.Actual.remove_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid self.yaks
           | _ ->
             Yaks_connector.Global.Actual.add_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fdu.fdu_id fdu.uuid fdu self.yaks >>= Lwt.return)
       | None -> Lwt.return_unit)
    | true ->
      (match( fduid,instanceid)  with
       | Some fduid , Some instanceid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id (Apero.Option.get self.configuration.agent.uuid) fduid instanceid self.yaks >>= Lwt.return
       | (_,_) -> Lwt.return_unit)



  (* Constrained Nodes Global *)
  (* let cb_gd_cnode_fdu self nodeid (fdu:FDU.record option) (is_remove:bool) (uuid:string option) =
     match is_remove with
     | false ->
     (match fdu with
     | Some fdu ->
     MVar.read self >>= fun self ->
     let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-CNODE-FDU - ##############") in
     let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-CNODE-FDU - FDU Updated! Agent will call the right plugin!") in
     let%lwt fdu_d = Yaks_connector.Global.Actual.get_fdu_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id fdu.fdu_uuid self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
     let fdu_type = Fos_sdk.string_of_hv_type fdu_d.hypervisor in
     let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-CNODE-FDU - FDU Type %s" fdu_type) in
     let%lwt plugins = Yaks_connector.LocalConstraint.Actual.get_node_plugins nodeid self.yaks in
     Lwt_list.iter_p (fun e ->
     let%lwt pl = Yaks_connector.LocalConstraint.Actual.get_node_plugin nodeid e self.yaks >>= fun x -> Lwt.return @@ Apero.Option.get x in
     if String.uppercase_ascii (pl.name) = String.uppercase_ascii (fdu_type) then
     let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-GD-FDU - Calling %s plugin" pl.name) in
     Yaks_connector.LocalConstraint.Desired.add_node_fdu nodeid pl.uuid fdu.fdu_uuid fdu self.yaks >>= Lwt.return
     else
     Lwt.return_unit
     ) plugins >>= Lwt.return
     | None -> Lwt.return_unit)
     | true ->
     (match uuid with
     | Some fduid -> MVar.read self >>= fun self ->
     Lwt.return_unit
     (* Yaks_connector.Global.Desired.remove_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id nodeid fduid self.yaks >>= Lwt.return *)
     | None -> Lwt.return_unit)
     in *)
  (* Constrained Nodes Local *)
  let cb_lac_node_fdu self _ (fdu:Infra.Descriptors.FDU.record option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match fdu with
       | Some fdu ->
         MVar.read self >>= fun _ ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-FDU - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-FDU - FDU Updated! Advertising on GA") in
         ( match fdu.status with
           | `UNDEFINE -> Lwt.return_unit
           (* Yaks_connector.Global.Actual.remove_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id nodeid fdu.fdu_uuid self.yaks *)
           | _ ->Lwt.return_unit)
       (* Yaks_connector.Global.Actual.add_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id nodeid fdu.fdu_uuid fdu self.yaks >>= Lwt.return) *)
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some _ -> MVar.read self >>= fun _ -> Lwt.return_unit
       (* Yaks_connector.Global.Actual.remove_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id nodeid fduid self.yaks >>= Lwt.return *)
       | None -> Lwt.return_unit)

  let cb_lac_plugin self nodeid (pl:FTypes.plugin option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match pl with
       | Some pl ->
         MVar.read self >>= fun self ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-PLUGIN - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-PLUGIN - Received plugin") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-PLUGIN - Name: %s" pl.name) in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-PLUGIN -  Plugin loaded advertising on GA") in
         Yaks_connector.Global.Actual.add_node_plugin Yaks_connector.default_system_id Yaks_connector.default_tenant_id nodeid pl self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some plid -> MVar.read self >>= fun self ->
         Yaks_connector.Global.Actual.remove_node_plugin Yaks_connector.default_system_id Yaks_connector.default_tenant_id nodeid plid self.yaks >>= Lwt.return
       | None -> Lwt.return_unit)

  (* let cb_lac_node_configuration self nodeid (pl:FTypes.node_info) =
     MVar.read self >>= fun self ->
     let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-CONF - ##############") in
     let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-CONF - Received plugin") in
     let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-CONF - Name: %s" pl.name) in
     let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NODE-CONF -  Plugin loaded advertising on GA") in
     Yaks_connector.Global.Actual.add_node_configuration Yaks_connector.default_system_id Yaks_connector.default_tenant_id nodeid pl self.yaks >>= Lwt.return
     in *)
  let cb_lac_nodes self (ni:FTypes.node_info option) (is_remove:bool) (uuid:string option) =
    match is_remove with
    | false ->
      (match ni with
       | Some ni ->
         MVar.guarded self @@ fun state ->
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NI - ##############") in
         let _ = Logs.debug (fun m -> m "[FOS-AGENT] - CB-LAC-NI - Updated node info advertising of GA") in
         let intid = ni.uuid in
         let extid = Apero.Uuid.to_string @@ Apero.Uuid.make () in
         let ext_ni = {ni with uuid = extid } in
         let%lwt _ = Yaks_connector.Global.Actual.add_node_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id extid ext_ni state.yaks in
         let%lwt _ = Yaks_connector.LocalConstraint.Actual.observe_node_plugins intid (cb_lac_plugin self  extid) state.yaks in
         let%lwt _ =  Yaks_connector.LocalConstraint.Actual.observe_node_fdu intid (cb_lac_node_fdu self extid) state.yaks in
         (* let%lwt _ = Yaks_connector.Global.Desired.observe_node_fdu Yaks_connector.default_system_id Yaks_connector.default_tenant_id extid (cb_gd_cnode_fdu self intid) state.yaks in *)
         MVar.return () {state with constrained_nodes = ConstraintMap.add extid intid state.constrained_nodes}
       | None -> Lwt.return_unit)
    | true ->
      (match uuid with
       | Some nid -> MVar.guarded self @@ fun state ->
         let%lwt _ = Yaks_connector.Global.Actual.remove_node_info Yaks_connector.default_system_id Yaks_connector.default_tenant_id nid state.yaks in
         MVar.return () {state with constrained_nodes =  ConstraintMap.remove nid state.constrained_nodes}
       | None -> Lwt.return_unit)