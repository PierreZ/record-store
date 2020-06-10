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

public class RecordStoreClient {
  private final String tenant;
  private final String container;
  private final String address;
  private final String token;
  private final BiscuitClientCredential credentials;
  private final ManagedChannel channel;
  private SchemaServiceGrpc.SchemaServiceFutureStub asyncSchemaStub;
  private RecordServiceGrpc.RecordServiceFutureStub asyncRecordStub;
  private AdminServiceGrpc.AdminServiceFutureStub asyncAdminStub;

  private RecordStoreClient(String tenant, String container, String address, String token) {
    this.tenant = tenant;
    this.container = container;
    this.address = address;
    this.token = token;
    credentials = new BiscuitClientCredential(tenant, token, container);

    // TODO: how to enable TLS
    channel = ManagedChannelBuilder.forTarget(this.address).usePlaintext().build();
    createCnx(channel);
  }


  private void createCnx(ManagedChannel channel) {
    asyncSchemaStub = SchemaServiceGrpc.newFutureStub(channel).withCallCredentials(credentials);
    asyncRecordStub = RecordServiceGrpc.newFutureStub(channel).withCallCredentials(credentials);
    asyncAdminStub = AdminServiceGrpc.newFutureStub(channel).withCallCredentials(credentials);
  }

  /**
   * Test connection to the record-store
   *
   * @return a future of an emptyResponse if everything is fine
   */
  public ListenableFuture<RecordStoreProtocol.EmptyResponse> ping() {
    return this.asyncAdminStub.ping(RecordStoreProtocol.EmptyRequest.newBuilder().build());
  }

  public ListenableFuture<RecordStoreProtocol.EmptyResponse> upsertSchema(RecordStoreProtocol.UpsertSchemaRequest request) {
    return this.asyncSchemaStub.upsert(request);
  }

  public ListenableFuture<RecordStoreProtocol.EmptyResponse> putRecord(Message record) {
    return this.putRecord(record.getClass().getSimpleName(), record.toByteArray());
  }

  public ListenableFuture<RecordStoreProtocol.EmptyResponse> putRecord(String recordTypeName, byte[] message) {
    return this.asyncRecordStub.put(RecordStoreProtocol.PutRecordRequest.newBuilder()
      .setMessage(ByteString.copyFrom(message))
      .setRecordTypeName(recordTypeName)
      .build());
  }

  public static class Builder {

    private String tenant;
    private String container;
    private String address;
    private String token;

    public Builder withTenant(String tenant) {
      this.tenant = tenant;
      return this;
    }

    public Builder withContainer(String container) {
      this.container = container;
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

    public RecordStoreClient build() {
      return new RecordStoreClient(tenant, container, address, token);
    }
  }
}
