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

import com.clevercloud.biscuit.error.Error;
import com.google.common.collect.ImmutableMap;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.vavr.control.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AuthInterceptor implements ServerInterceptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthInterceptor.class);
  private static final Map<Metadata.Key<String>, Context.Key<String>> METADATA_KEY_TO_CONTEXT_KEY = ImmutableMap.of(
    GrpcMetadataKeys.RECORDSPACE_METADATA_KEY, GrpcContextKeys.CONTAINER_NAME,
    GrpcMetadataKeys.TENANT_METADATA_KEY, GrpcContextKeys.TENANT_ID_KEY
  );
  private final BiscuitManager biscuitManager;
  private final List<Metadata.Key<String>> requiredKeys = Arrays.asList(
    GrpcMetadataKeys.AUTHORIZATION_METADATA_KEY,
    GrpcMetadataKeys.RECORDSPACE_METADATA_KEY,
    GrpcMetadataKeys.TENANT_METADATA_KEY
  );

  public AuthInterceptor(String key) {
    biscuitManager = new BiscuitManager(key);
  }

  /**
   * Intercept {@link ServerCall} dispatch by the {@code next} {@link ServerCallHandler}. General
   * semantics of {@link ServerCallHandler#startCall} apply and the returned
   * {@link ServerCall.Listener} must not be {@code null}.
   *
   * <p>If the implementation throws an exception, {@code call} will be closed with an error.
   * Implementations must not throw an exception if they started processing that may use {@code
   * call} on another thread.
   *
   * @param call    object to receive response messages
   * @param headers which can contain extra call metadata from {@link ClientCall#start},
   *                e.g. authentication credentials.
   * @param next    next processor in the interceptor chain
   * @return listener for processing incoming messages for {@code call}, never {@code null}.
   */
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    Context context = Context.current();
    LOGGER.info("{}", headers);

    if (!headers.containsKey(GrpcMetadataKeys.AUTHORIZATION_METADATA_KEY)) {
      call.close(Status.PERMISSION_DENIED.withDescription("no authorization token"), new Metadata());
      return new ServerCall.Listener<ReqT>() {
      };
    }

    String tenant = getFromHeaders(headers, GrpcMetadataKeys.TENANT_METADATA_KEY);
    context = context.withValue(GrpcContextKeys.TENANT_ID_KEY, tenant);

    if (tenant == null) {
      call.close(Status.PERMISSION_DENIED.withDescription("no tenant provided"), new Metadata());
      return new ServerCall.Listener<ReqT>() {
      };
    }

    String token = headers.get(GrpcMetadataKeys.AUTHORIZATION_METADATA_KEY);
    if (token == null) {
      call.close(Status.PERMISSION_DENIED.withDescription("no token provided"), new Metadata());
      return new ServerCall.Listener<ReqT>() {
      };
    }
    if (!token.startsWith("Bearer ")) {
      call.close(Status.PERMISSION_DENIED.withDescription("expected format 'Bearer my-token'"), new Metadata());
      return new ServerCall.Listener<ReqT>() {
      };
    }

    Either<Error, Void> result = this.biscuitManager.checkTenant(tenant, token.substring("Bearer ".length()));
    if (result.isLeft()) {
      call.close(Status.UNAUTHENTICATED.withDescription("bad tenant and/or token"), headers);
      return new ServerCall.Listener<ReqT>() {
      };
    }

    // Admin calls does not need recordSpaces
    if (call.getMethodDescriptor().getFullMethodName().toLowerCase().contains("admin")) {
      return Contexts.interceptCall(context, call, headers, next);
    }

    String recordSpace = getFromHeaders(headers, GrpcMetadataKeys.RECORDSPACE_METADATA_KEY);
    context = context.withValue(GrpcContextKeys.CONTAINER_NAME, recordSpace);

    return Contexts.interceptCall(context, call, headers, next);
  }

  private String getFromHeaders(Metadata headers, Metadata.Key<String> tenantMetadataKey) {
    if (headers.containsKey(tenantMetadataKey)) {
      return headers.get(tenantMetadataKey);
    }
    return null;
  }
}
