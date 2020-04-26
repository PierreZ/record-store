package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBMetaDataStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import fr.pierrezemb.recordstore.fdb.RSMetaDataStore;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.Collections;

public class SchemaService extends SchemaServiceGrpc.SchemaServiceImplBase {
  // Keep a global track of the number of records stored
  protected static final Index COUNT_INDEX = new Index(
    "globalRecordCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
  private final FDBDatabase db;
  private final FDBStoreTimer timer;

  public SchemaService(FDBDatabase db, FDBStoreTimer fdbStoreTimer) {
    this.db = db;
    this.timer = fdbStoreTimer;
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void create(RecordStoreProtocol.CreateSchemaRequest request, StreamObserver<RecordStoreProtocol.CreateSchemaResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID);

      // retrieving protobuf descriptor
      RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder();
      DescriptorProtos.FileDescriptorSet descriptorSet = request.getSchema().getDescriptorSet();
      for (DescriptorProtos.FileDescriptorProto fdp : descriptorSet.getFileList()) {
        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(fdp, new Descriptors.FileDescriptor[]{});
        metadataBuilder.setRecords(fd);
      }

      // set primary key
      metadataBuilder.getRecordType(request.getName()).setPrimaryKey(Key.Expressions.field(request.getPrimaryKeyField()));

      // add internal indexes
      metadataBuilder.addIndex(request.getName(), COUNT_INDEX);

      // and save it
      metaDataStore.saveRecordMetaData(metadataBuilder.getRecordMetaData().toProto());
      context.commit();

    } catch (Descriptors.DescriptorValidationException e) {
      responseObserver.onError(e);
      responseObserver.onCompleted();
    }

    responseObserver.onNext(RecordStoreProtocol.CreateSchemaResponse.newBuilder().setResult(RecordStoreProtocol.Result.OK).build());
    responseObserver.onCompleted();
  }
}
