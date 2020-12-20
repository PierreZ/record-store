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
package fr.pierrezemb.recordstore.grpc;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class GrpcContextKeys {
  /** Key for accessing requested tenant id */
  public static final Context.Key<String> TENANT_ID_KEY = Context.key("tenant");

  public static final Context.Key<String> CONTAINER_NAME = Context.key("recordSpace");

  public static String getTenantIDOrFail() throws StatusRuntimeException {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("missing tenant"));
    }
    return tenantId;
  }

  public static String getContainerOrFail() throws StatusRuntimeException {
    String recordSpace = GrpcContextKeys.CONTAINER_NAME.get();
    if (recordSpace == null) {
      throw new StatusRuntimeException(
          Status.FAILED_PRECONDITION.withDescription("missing recordSpace"));
    }
    return recordSpace;
  }
}
