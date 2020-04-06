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


open Fos_sdk


type agent_state = {
  yaks : Yaks_connector.connector
; configuration : Fos_sdk.configuration
; cli_parameters : string list
; completer : bool Lwt.u
; constrained_nodes : string ConstraintMap.t
; fim_api : Fos_fim_api.api
}

type t = agent_state MVar.t