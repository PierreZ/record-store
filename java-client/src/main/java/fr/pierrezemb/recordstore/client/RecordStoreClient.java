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
package fr.pierrezemb.recordstore.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import fr.pierrezemb.recordstore.auth.BiscuitClientCredential;
import fr.pierrezemb.recordstore.proto.AdminServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RecordStoreClient {
  private final String tenant;
  private final String recordSpace;
  private final String address;
  private final String token;
  private final BiscuitClientCredential credentials;
  private final ManagedChannel channel;
  private final SchemaServiceGrpc.SchemaServiceFutureStub asyncSchemaStub;
  private final RecordServiceGrpc.RecordServiceFutureStub asyncRecordStub;
  private final AdminServiceGrpc.AdminServiceFutureStub asyncAdminStub;
  private final RecordServiceGrpc.RecordServiceBlockingStub syncRecordStub;

  private RecordStoreClient(String tenant, String recordSpace, String address, String token)
      throws InterruptedException, ExecutionException, TimeoutException {
    this.tenant = tenant;
    this.recordSpace = recordSpace;
    this.address = address;
    this.token = token;
    credentials = new BiscuitClientCredential(tenant, token, recordSpace);

    // TODO: how to enable TLS
    channel = ManagedChannelBuilder.forTarget(this.address).usePlaintext().build();

    asyncSchemaStub = SchemaServiceGrpc.newFutureStub(channel).withCallCredentials(credentials);
    syncRecordStub = RecordServiceGrpc.newBlockingStub(channel).withCallCredentials(credentials);
    asyncRecordStub = RecordServiceGrpc.newFutureStub(channel).withCallCredentials(credentials);
    asyncAdminStub = AdminServiceGrpc.newFutureStub(channel).withCallCredentials(credentials);

    this.ping().get(1, TimeUnit.SECONDS);
  }

  /**
   * Test connection to the record-store
   *
   * @return a future of an emptyResponse if everything is fine
   */
  public ListenableFuture<RecordStoreProtocol.EmptyResponse> ping() {
    return this.asyncAdminStub.ping(RecordStoreProtocol.EmptyRequest.newBuilder().build());
  }

  public ListenableFuture<RecordStoreProtocol.EmptyResponse> upsertSchema(
      RecordStoreProtocol.UpsertSchemaRequest request) {
    return this.asyncSchemaStub.upsert(request);
  }

  public ListenableFuture<RecordStoreProtocol.EmptyResponse> putRecord(Message record) {
    return this.putRecord(record.getClass().getSimpleName(), record.toByteArray());
  }

  public ListenableFuture<RecordStoreProtocol.EmptyResponse> putRecord(
      String recordTypeName, byte[] message) {
    return this.asyncRecordStub.put(
        RecordStoreProtocol.PutRecordRequest.newBuilder()
            .setMessage(ByteString.copyFrom(message))
            .setRecordTypeName(recordTypeName)
            .build());
  }

  public ListenableFuture<RecordStoreProtocol.StatResponse> getStats() {
    return asyncSchemaStub.stat(RecordStoreProtocol.StatRequest.newBuilder().build());
  }

  public Iterator<RecordStoreProtocol.QueryResponse> queryRecords(
      RecordStoreProtocol.QueryRequest request) {
    return syncRecordStub.query(request);
  }

  public static class Builder {

    private String tenant;
    private String recordSpace;
    private String address;
    private String token;

    public Builder withTenant(String tenant) {
      this.tenant = tenant;
      return this;
    }

    public Builder withRecordSpace(String recordSpace) {
      this.recordSpace = recordSpace;
      return this;
    }

    public Builder withAddress(String address) {
      this.address = address;
      return this;
    }

    public Builder withToken(String token) {
      this.token = token;
      return this;
    }

    public RecordStoreClient connect()
        throws InterruptedException, ExecutionException, TimeoutException {
      return new RecordStoreClient(tenant, recordSpace, address, token);
    }
  }
}
