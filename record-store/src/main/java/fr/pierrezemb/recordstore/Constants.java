/**
 * Copyright 2020 Pierre Zemb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.pierrezemb.recordstore;

public class Constants {
  public static final String CONFIG_ENCRYPTION_KEY_DEFAULT = "6B58703273357638792F423F4528482B";
  public static final String CONFIG_BISCUIT_KEY_DEFAULT = "3A8621F1847F19D6DAEAB5465CE8D3908B91C66FB9AF380D508FCF9253458907";

  public static final String CONFIG_FDB_CLUSTER_FILE = "fdb-cluster-file";
  public static final String CONFIG_FDB_CLUSTER_FILE_DEFAULT = "/var/fdb/fdb.cluster";
  public static final String CONFIG_BISCUIT_KEY = "biscuit-key";
  public static final String CONFIG_ENCRYPTION_KEY = "encryption-key";

  public static final String CONFIG_GRPC_LISTEN_ADDRESS = "grpc-listen-address";
  public static final String CONFIG_GRPC_LISTEN_PORT = "grpc-listen-port";

  public static final String CONFIG_GRAPHQL_LISTEN_PORT = "graphql-listen-port";
  public static final String CONFIG_LOAD_DEMO = "load-demo";

  public static final String CONFIG_ENABLE_MANAGED_KV = "enable-managed-kv";
  public static final Boolean CONFIG_ENABLE_MANAGED_KV_DEFAULT = true;
}
