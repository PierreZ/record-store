package fr.pierrezemb.recordstore.grpc;

import io.grpc.Context;

public class GrpcContextKeys {
  /**
   * Key for accessing requested tenant id
   */
  public static final Context.Key<String> TENANT_ID_KEY = Context.key("tenant");
}
