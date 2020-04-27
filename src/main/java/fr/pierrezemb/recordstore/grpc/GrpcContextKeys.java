package fr.pierrezemb.recordstore.grpc;

import io.grpc.Context;

public class GrpcContextKeys {
  /**
   * Key for accessing requested tenant id
   */
  public static final Context.Key<String> TENANT_ID_KEY = Context.key("tenant");
  public static final Context.Key<String> ENV_KEY = Context.key("env");

  public static String getTenantIDOrFail() throws RuntimeException {
    String tenantId = GrpcContextKeys.TENANT_ID_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Auth enabled and tenant not found.");
    }
    return tenantId;
  }

  public static String getEnvOrFail() throws RuntimeException {
    String tenantId = GrpcContextKeys.ENV_KEY.get();
    if (tenantId == null) {
      throw new RuntimeException("Env not found");
    }
    return tenantId;
  }
}
