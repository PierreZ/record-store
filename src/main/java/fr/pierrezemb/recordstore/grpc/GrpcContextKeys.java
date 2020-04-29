package fr.pierrezemb.recordstore.grpc;

import io.grpc.Context;

public class GrpcContextKeys {
  /**
   * Key for accessing requested tenant id
   */
  public static final Context.Key<String> TENANT_ID_KEY = Context.key("tenant");
  public static final Context.Key<String> CONTAINER_NAME = Context.key("container");

  public static String getTenantIDOrFail() throws RuntimeException {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }
    return tenantId;
  }

  public static String getContainerOrFail() throws RuntimeException {
    String container = GrpcContextKeys.CONTAINER_NAME.get();
    if (container == null) {
      throw new RuntimeException("container not found");
    }
    return container;
  }
}
