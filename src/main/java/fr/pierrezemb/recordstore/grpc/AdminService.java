package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolvedKeySpacePath;
import fr.pierrezemb.recordstore.fdb.RSKeySpace;
import fr.pierrezemb.recordstore.proto.AdminServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AdminService extends AdminServiceGrpc.AdminServiceImplBase {
  private final FDBDatabase db;
  private final FDBStoreTimer timer;

  public AdminService(FDBDatabase db, FDBStoreTimer timer) {
    this.db = db;
    this.timer = timer;
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void list(RecordStoreProtocol.ListContainerRequest request, StreamObserver<RecordStoreProtocol.ListContainerResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      KeySpacePath tenantKeySpace = RSKeySpace.getApplicationKeySpacePath(tenantID);
      System.out.println(tenantKeySpace);
      List<ResolvedKeySpacePath> containers = tenantKeySpace
        .listSubdirectory(context, "container", ScanProperties.FORWARD_SCAN);
      List<String> result = containers.stream()
        .map(e -> e.getResolvedValue().toString())
        .collect(Collectors.toList());

      responseObserver.onNext(RecordStoreProtocol.ListContainerResponse.newBuilder()
        .addAllContainers(result)
        .build());
      responseObserver.onCompleted();
    }
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void delete(RecordStoreProtocol.DeleteContainerRequest request, StreamObserver<RecordStoreProtocol.DeleteContainerResponse> responseObserver) {
    super.delete(request, responseObserver);
  }
}
