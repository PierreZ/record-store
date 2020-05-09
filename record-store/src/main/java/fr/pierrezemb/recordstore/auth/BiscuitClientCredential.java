package fr.pierrezemb.recordstore.auth;

import io.grpc.Metadata;
import io.grpc.Status;

import java.util.concurrent.Executor;

import static fr.pierrezemb.recordstore.grpc.GrpcMetadataKeys.*;

public class BiscuitClientCredential extends io.grpc.CallCredentials {
  static final String BEARER_TYPE = "Bearer";

  private final String tenant;
  private final String biscuit;
  private final String container;

  public BiscuitClientCredential(String tenant, String sealedBiscuit, String container) {
    this.tenant = tenant;
    this.biscuit = sealedBiscuit;
    this.container = container;
  }

  @Override
  public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    appExecutor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          Metadata headers = new Metadata();
          headers.put(AUTHORIZATION_METADATA_KEY, String.format("%s %s", BEARER_TYPE, biscuit));
          headers.put(CONTAINER_METADATA_KEY, container);
          headers.put(TENANT_METADATA_KEY, tenant);
          applier.apply(headers);

        } catch (Throwable e) {
          applier.fail(Status.UNAUTHENTICATED.withCause(e));
        }
      }
    });
  }

  /**
   * Should be a noop but never called; tries to make it clearer to implementors that they may break
   * in the future.
   */
  @Override
  public void thisUsesUnstableApi() {
    // noop
  }
}
