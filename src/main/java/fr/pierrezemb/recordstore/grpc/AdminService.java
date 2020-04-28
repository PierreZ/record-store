package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import fr.pierrezemb.recordstore.fdb.RSKeySpace;
import fr.pierrezemb.recordstore.proto.AdminServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import io.grpc.stub.StreamObserver;
import java.util.Collections;

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
  public void deleteAll(RecordStoreProtocol.DeleteAllRequest request, StreamObserver<RecordStoreProtocol.DeleteAllResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String env = GrpcContextKeys.getEnvOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      FDBRecordStore.deleteStore(context, RSKeySpace.getDataKeySpacePath(tenantID, env));
      FDBRecordStore.deleteStore(context, RSKeySpace.getMetaDataKeySpacePath(tenantID, env));
      context.commit();
    }
    responseObserver.onNext(RecordStoreProtocol.DeleteAllResponse.newBuilder().setResult(RecordStoreProtocol.Result.OK).build());
    responseObserver.onCompleted();
  }
}
