package fr.pierrezemb.recordstore.grpc;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class GrpcContextKeys {
  /**
   * Key for accessing requested tenant id
   */
  public static final Context.Key<String> TENANT_ID_KEY = Context.key("tenant");
  public static final Context.Key<String> CONTAINER_NAME = Context.key("container");

  public static String getTenantIDOrFail() throws StatusRuntimeException {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("missing tenant"));
    }
    return tenantId;
  }

  public static String getContainerOrFail() throws StatusRuntimeException {
    String container = GrpcContextKeys.CONTAINER_NAME.get();
    if (container == null) {
      throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("missing container"));
    }
    return container;
  }
}
