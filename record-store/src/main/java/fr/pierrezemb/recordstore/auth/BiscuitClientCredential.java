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
package fr.pierrezemb.recordstore.auth;

import static fr.pierrezemb.recordstore.grpc.GrpcMetadataKeys.AUTHORIZATION_METADATA_KEY;
import static fr.pierrezemb.recordstore.grpc.GrpcMetadataKeys.RECORDSPACE_METADATA_KEY;
import static fr.pierrezemb.recordstore.grpc.GrpcMetadataKeys.TENANT_METADATA_KEY;

import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.Executor;

public class BiscuitClientCredential extends io.grpc.CallCredentials {
  static final String BEARER_TYPE = "Bearer";

  private final String tenant;
  private final String biscuit;
  private final String recordSpace;

  public BiscuitClientCredential(String tenant, String sealedBiscuit, String recordSpace) {
    this.tenant = tenant;
    this.biscuit = sealedBiscuit;
    this.recordSpace = recordSpace;
  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    appExecutor.execute(
        new Runnable() {
          @Override
          public void run() {
            try {
              Metadata headers = new Metadata();
              headers.put(AUTHORIZATION_METADATA_KEY, String.format("%s %s", BEARER_TYPE, biscuit));
              headers.put(RECORDSPACE_METADATA_KEY, recordSpace);
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
