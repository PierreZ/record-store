package fr.pierrezemb.recordstore.grpc;

import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import io.grpc.stub.StreamObserver;

public class SchemaService extends SchemaServiceGrpc.SchemaServiceImplBase {
  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void create(RecordStoreProtocol.CreateSchemaRequest request, StreamObserver<RecordStoreProtocol.CreateSchemaResponse> responseObserver) {
    super.create(request, responseObserver);
  }
}
