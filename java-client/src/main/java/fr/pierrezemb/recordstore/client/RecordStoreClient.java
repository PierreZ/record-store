package fr.pierrezemb.recordstore.client;

import fr.pierrezemb.recordstore.proto.RecordServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class RecordStoreClient {
  private final String tenant;
  private final String container;
  private final String address;
  private final String token;

  public RecordStoreClient(String tenant, String container, String address, String token) {
    this.tenant = tenant;
    this.container = container;
    this.address = address;
    this.token = token;
  }
}
