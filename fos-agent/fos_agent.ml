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
(* open Fos_sdk.Errors *)
open Agent_state
(* open Fos_errors *)


let register_handlers completer () =
  try%lwt
    let _ = Lwt_unix.on_signal Sys.sigint (fun _ -> Logs.debug (fun m -> m "[register_handlers] Received SIGINT"); Lwt.wakeup_later completer false) in
    let _ = Lwt_unix.on_signal Sys.sigterm (fun _ ->Logs.debug (fun m -> m "[register_handlers] Received SIGTERM"); Lwt.wakeup_later completer false) in
    Lwt.return_unit
  with
    | exn ->
      Logs.err (fun m -> m "[register_handlers] Exception %s raised:\n%s" (Printexc.to_string exn) (Printexc.get_backtrace ()));
      Lwt.return_unit

(* MAIN *)

let main_loop state promise ping_completer heartbeat_completer =
  Logs.info (fun m -> m "Eclipse fog05 Agent - Up & Running!");
  promise >>= fun run ->
  Logs.debug (fun m -> m "[main_loop] - Completer filled with %b" run);
  Logs.debug (fun m -> m "[main_loop] - Received signal... shutdown agent");
  Lwt.wakeup_later ping_completer false;
  Lwt.wakeup_later heartbeat_completer false;
  MVar.guarded state @@ fun self ->
  (* Here we should store all information in a persistent YAKS
   * and remove them from the in memory YAKS
  *)
  Yaks_connector.close_connector self.yaks
  >>= fun _ ->
  Logs.info (fun m -> m "Eclipse fog05 Agent - Bye!");
  MVar.return () self



let agent verbose_flag debug_flag configuration custom_uuid hb_flag =
  Random.self_init();
  let level, reporter = (match verbose_flag with
      | true -> Apero.Result.get @@ Logs.level_of_string "debug" ,  (Logs_fmt.reporter ())
      | false -> Apero.Result.get @@ Logs.level_of_string "error",  (Fos_sdk.get_unix_syslog_reporter ())
    )
  in

  Logs.set_level level;
  Logs.set_reporter reporter;
  let prom,c = Lwt.wait () in
  let _ = register_handlers c () in
  let conf = load_config configuration custom_uuid in
  let conf = match conf.agent.system with
    | Some _ -> conf
    | None -> {conf with agent = {conf.agent with uuid = Some( Yaks_connector.default_system_id )} }
  in
  let mpid = Unix.getpid () in
  let pid_out = open_out conf.agent.pid_file in
  ignore @@ Printf.fprintf pid_out "%d" mpid;
  ignore @@ close_out pid_out;
  let sys_id = Apero.Option.get @@ conf.agent.system in
  let uuid = (Apero.Option.get conf.agent.uuid) in
  Logs.debug (fun m -> m "[agent] - INIT - DEBUG IS: %b"  debug_flag);
  Logs.debug (fun m -> m "[agent] - INIT - Heartbeat IS: %b"  hb_flag);
  Logs.debug (fun m -> m "[agent] - INIT - ##############");
  Logs.debug (fun m -> m "[agent] - INIT - Agent Configuration is:");
  Logs.debug (fun m -> m "[agent] - INIT - SYSID: %s" sys_id);
  Logs.debug (fun m -> m "[agent] - INIT - UUID: %s"  uuid);
  Logs.debug (fun m -> m "[agent] - INIT - LLDPD: %b" conf.agent.enable_lldp);
  Logs.debug (fun m -> m "[agent] - INIT - SPAWNER: %b" conf.agent.enable_spawner);
  Logs.debug (fun m -> m "[agent] - INIT - PID FILE: %s" conf.agent.pid_file);
  Logs.debug (fun m -> m "[agent] - INIT - YAKS Server: %s" conf.agent.yaks);
  Logs.debug (fun m -> m "[agent] - INIT - MGMT Interface: %s" conf.agent.mgmt_interface);
  (* let sys_info = system_info sys_id uuid in *)
  let%lwt yaks = Yaks_connector.get_connector conf in
  let%lwt fim = Fos_fim_api.FIMAPI.connect ~locator:conf.agent.yaks () in
  (*
   * Here we should check if state is present in local persistent YAKS and
   * recoved from that
   *)
  let cli_parameters = [configuration] in
  let self = {yaks; configuration = conf; cli_parameters; completer = c; constrained_nodes = ConstraintMap.empty; fim_api = fim; available_nodes = []} in
  let state = MVar.create self in
  let%lwt _ = MVar.read state >>= fun state ->
    Yaks_connector.Global.Actual.add_node_configuration sys_id Yaks_connector.default_tenant_id uuid conf state.yaks
  in
  let%lwt _ = MVar.read state >>= fun state ->
    Yaks_connector.Local.Actual.add_node_configuration uuid conf state.yaks
  in
  (* Registering Evals *)
  (* FDU Evals *)
  let%lwt _ = Yaks_connector.Local.Actual.add_agent_eval uuid "get_fdu_info" (Evals.eval_get_fdu_info state) yaks in
  let%lwt _ = Yaks_connector.Local.Actual.add_agent_eval uuid "get_node_fdu_info" (Evals.eval_get_node_fdu_info state) yaks in
  (* Node evals *)
  let%lwt _ = Yaks_connector.Local.Actual.add_agent_eval uuid "get_node_mgmt_address" (Evals.eval_get_node_mgmt_address state) yaks in
  (* Network/Port/Image evals *)
  let%lwt _ = Yaks_connector.Local.Actual.add_agent_eval uuid "get_network_info" (Evals.eval_get_network_info state) yaks in
  let%lwt _ = Yaks_connector.Local.Actual.add_agent_eval uuid "get_port_info" (Evals.eval_get_port_info state) yaks in
  let%lwt _ = Yaks_connector.Local.Actual.add_agent_eval uuid "get_image_info" (Evals.eval_get_image_info state) yaks in
  (* Network Mgmt Evals *)
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "create_node_network" (Evals.eval_create_net state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "remove_node_network" (Evals.eval_remove_net state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "create_cp" (Evals.eval_create_cp state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "remove_cp" (Evals.eval_remove_cp state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "connect_cp_to_face" (Evals.eval_connect_cp_to_fdu_face state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "disconnect_cp_from_face" (Evals.eval_disconnect_cp_from_fdu_face state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "add_port_to_network" (Evals.eval_connect_cp_to_network state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "remove_port_from_network" (Evals.eval_remove_cp_from_network state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "create_floating_ip" (Evals.eval_create_floating_ip state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "delete_floating_ip" (Evals.eval_delete_floating_ip state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "assign_floating_ip" (Evals.eval_assign_floating_ip state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "remove_floating_ip" (Evals.eval_remove_floating_ip state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "add_router_port" (Evals.eval_add_router_port state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "remove_router_port" (Evals.eval_remove_router_port state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "node_interface_to_bridge" (Evals.eval_attach_to_node_bridge uuid state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "node_interface_no_bridge" (Evals.eval_detach_from_node_bridge uuid state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "node_create_bridge" (Evals.eval_add_node_bridge uuid state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "node_remove_bridge" (Evals.eval_remove_node_bridge uuid state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "node_create_vxlan" (Evals.eval_add_node_vxlan uuid state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "node_delete_vxlan" (Evals.eval_remove_node_vxlan uuid state) yaks in
  (* FDU Evals *)
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "onboard_fdu" (Evals.eval_onboard_fdu state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "define_fdu" (Evals.eval_define_fdu state) yaks in
  (* Scheduler Evals *)
  let%lwt _ = Yaks_connector.Global.Actual.add_fdu_check_eval sys_id Yaks_connector.default_tenant_id uuid (Evals.eval_check_fdu state) yaks in
  let%lwt _ = Yaks_connector.Global.Actual.add_fdu_schedule_eval sys_id Yaks_connector.default_tenant_id uuid (Evals.eval_schedule_fdu state) yaks in
  (* Heartbeat eval *)
  let%lwt _ = Yaks_connector.Global.Actual.add_agent_eval sys_id Yaks_connector.default_tenant_id uuid "heartbeat" (Evals.eval_heartbeat uuid state) yaks in
  (* Constraint Eval  *)
  let%lwt _ = Yaks_connector.LocalConstraint.Actual.add_agent_eval uuid "get_fdu_info" (Evals.eval_get_fdu_info state) yaks in
  (* Registering listeners *)
  (* Global Desired Listeners *)
  let%lwt _ = Yaks_connector.Global.Desired.observe_node_plugins sys_id Yaks_connector.default_tenant_id uuid (Listeners.cb_gd_plugin state) yaks in
  let%lwt _ = Yaks_connector.Global.Desired.observe_catalog_fdu sys_id Yaks_connector.default_tenant_id (Listeners.cb_gd_fdu state) yaks in
  let%lwt _ = Yaks_connector.Global.Desired.observe_node_fdu sys_id Yaks_connector.default_tenant_id uuid (Listeners.cb_gd_node_fdu state) yaks in
  let%lwt _ = Yaks_connector.Global.Desired.observe_network sys_id Yaks_connector.default_tenant_id (Listeners.cb_gd_net_all state) yaks in
  (* let%lwt _ = Yaks_connector.Global.Desired.observe_node_network sys_id Yaks_connector.default_tenant_id uuid (Listeners.cb_gd_net state) yaks in *)
  let%lwt _ = Yaks_connector.Global.Desired.observe_ports sys_id Yaks_connector.default_tenant_id (Listeners.cb_gd_cp state) yaks in
  let%lwt _ = Yaks_connector.Global.Desired.observe_images sys_id Yaks_connector.default_tenant_id (Listeners.cb_gd_image state) yaks in
  let%lwt _ = Yaks_connector.Global.Desired.observe_flavors sys_id Yaks_connector.default_tenant_id (Listeners.cb_gd_flavor state) yaks in
  (* Global Actual with NodeID *)
  let%lwt _ = Yaks_connector.Global.Desired.observe_node_routers sys_id Yaks_connector.default_tenant_id uuid (Listeners.cb_gd_router state) yaks in
  (* Global Actual *)
  (* let%lwt _ = Yaks_connector.Global.Actual.observe_nodes sys_id Yaks_connector.default_tenant_id (Listeners.cb_ga_nodes state) yaks in *)
  (* Local Actual Listeners *)
  let%lwt _ = Yaks_connector.Local.Actual.observe_node_plugins uuid (Listeners.cb_la_plugin state) yaks in
  let%lwt _ = Yaks_connector.Local.Actual.observe_node_info uuid (Listeners.cb_la_ni state) yaks in
  let%lwt _ = Yaks_connector.Local.Actual.observe_node_status uuid (Listeners.cb_la_ns state) yaks in
  let%lwt _ = Yaks_connector.Local.Actual.observe_node_fdu uuid (Listeners.cb_la_node_fdu state) yaks in
  let%lwt _ = Yaks_connector.Local.Actual.observe_node_network uuid (Listeners.cb_la_net state) yaks in
  let%lwt _ = Yaks_connector.Local.Actual.observe_node_port uuid (Listeners.cb_la_cp state) yaks in
  let%lwt _ = Yaks_connector.Local.Actual.observe_node_router uuid (Listeners.cb_la_router state) yaks in
  let%lwt _ = Yaks_connector.LocalConstraint.Actual.observe_nodes (Listeners.cb_lac_nodes state) yaks in
  (* Starting Ping Server, listening on all interfaces, on port 9091  *)
  let _ = Heartbeat.run_server (Lwt_unix.ADDR_INET ((Unix.inet_addr_of_string "0.0.0.0"), 9091)) uuid in

  let%lwt c_p, c_h = match hb_flag with
    | true ->

      let%lwt c_p = Utils.start_ping_single_threaded uuid yaks in
      let%lwt c_h = Utils.heartbeat_task state in
      Lwt.return (c_p,c_h)
    | false ->
      let _,c_p = Lwt.wait () in
      let _,c_h = Lwt.wait () in
      Lwt.return (c_p,c_h)
  in

  main_loop state prom c_p c_h

(* AGENT CMD  *)

let start verbose_flag debug_flag configuration_path custom_uuid hb_flag =
  Lwt_main.run @@ agent verbose_flag debug_flag configuration_path custom_uuid hb_flag

let verbose =
  let doc = "Set verbose output." in
  Cmdliner.Arg.(value & flag & info ["v";"verbose"] ~doc)

let id =
  let doc = "Set custom node ID" in
  Cmdliner.Arg.(value & opt (some string) None & info ["i";"id"] ~doc)

let debug =
  let doc = "Set debug (not load spawner)" in
  Cmdliner.Arg.(value & flag & info ["b";"debug"] ~doc)

let hb =
  let doc = "Disable heartbeat" in
  Cmdliner.Arg.(value & flag & info ["h";"heartbeat"] ~doc)

let config =
  let doc = "Configuration file path" in
  Cmdliner.Arg.(value & opt string "/etc/fos/agent.json" & info ["c";"conf"] ~doc)


let agent_t = Cmdliner.Term.(const start $ verbose $ debug $ config $ id $ hb)

let info =
  let doc = "fog05 | The Fog-Computing IaaS" in
  let man = [
    `S Cmdliner.Manpage.s_bugs;
    `P "Email bug reports to fog05-dev at eclipse.org>." ]
  in
  Cmdliner.Term.info "agent" ~version:"%%VERSION%%" ~doc ~exits:Cmdliner.Term.default_exits ~man


let () =
  Printexc.record_backtrace true;
  Cmdliner.Term.exit @@ Cmdliner.Term.eval (agent_t, info)