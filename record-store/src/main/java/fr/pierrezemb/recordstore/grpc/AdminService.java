package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolvedKeySpacePath;
import fr.pierrezemb.recordstore.fdb.RecordStoreKeySpace;
import fr.pierrezemb.recordstore.proto.AdminServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AdminService extends AdminServiceGrpc.AdminServiceImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdminService.class);
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
      KeySpacePath tenantKeySpace = RecordStoreKeySpace.getApplicationKeySpacePath(tenantID);
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
  public void delete(RecordStoreProtocol.DeleteContainerRequest request, StreamObserver<RecordStoreProtocol.EmptyResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      for (String container : request.getContainersList()) {
        FDBRecordStore.deleteStore(context, RecordStoreKeySpace.getDataKeySpacePath(tenantID, container));
        FDBRecordStore.deleteStore(context, RecordStoreKeySpace.getMetaDataKeySpacePath(tenantID, container));
        context.commit();
      }
    } catch (RuntimeException runtimeException) {
      LOGGER.error("could not delete container", runtimeException);
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(runtimeException.getMessage()));
    }

    responseObserver.onNext(RecordStoreProtocol.EmptyResponse.newBuilder()
      .build());
    responseObserver.onCompleted();
  }

  @Override
  public void ping(RecordStoreProtocol.EmptyRequest request, StreamObserver<RecordStoreProtocol.EmptyResponse> responseObserver) {
    GrpcContextKeys.getTenantIDOrFail();
    responseObserver.onNext(RecordStoreProtocol.EmptyResponse.newBuilder().build());
    responseObserver.onCompleted();
  }
}
