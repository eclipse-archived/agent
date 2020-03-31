open Fos_sdk


type state = {
  yaks : Yaks_connector.connector
; spawner : Lwt_process.process_full option
; configuration : Fos_sdk.configuration
; cli_parameters : string list
; completer : unit Lwt.u
; constrained_nodes : string ConstraintMap.t
; fim_api : Fos_fim_api.api
(* ; faem_api : Fos_faem_api.faemapi *)
}

type t = state MVar.t