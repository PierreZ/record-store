package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBMetaDataStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import fr.pierrezemb.recordstore.fdb.RSKeySpace;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import io.grpc.stub.StreamObserver;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SchemaService extends SchemaServiceGrpc.SchemaServiceImplBase {
  private final FDBDatabase db;

  public SchemaService(FDBDatabase db) {
    this.db = db;
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void create(RecordStoreProtocol.CreateSchemaRequest request, StreamObserver<RecordStoreProtocol.CreateSchemaResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();

    try (FDBRecordContext context = db.openContext()) {
      KeySpacePath metaDataPath = RSKeySpace.getMetaDataKeySpacePath(tenantID);
      FDBMetaDataStore metaDataStore = createMetaDataStore(context, metaDataPath, null);

      // retrieving protobuf descriptor
      RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder();
      DescriptorProtos.FileDescriptorSet descriptorSet = request.getSchema().getDescriptorSet();
      for (DescriptorProtos.FileDescriptorProto fdp : descriptorSet.getFileList()) {
        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(fdp, new Descriptors.FileDescriptor[]{});
        metadataBuilder.setRecords(fd);
      }

      // set primary key
      metadataBuilder.getRecordType(request.getName()).setPrimaryKey(Key.Expressions.field(request.getPrimaryKeyField()));

      // and save it
      metaDataStore.saveAndSetCurrent(metadataBuilder.getRecordMetaData().toProto()).join();

    } catch (Descriptors.DescriptorValidationException e) {
      responseObserver.onError(e);
      responseObserver.onCompleted();
    }

    responseObserver.onNext(RecordStoreProtocol.CreateSchemaResponse.newBuilder().setResult(RecordStoreProtocol.Result.OK).build());
    responseObserver.onCompleted();
  }

  @Nonnull
  private FDBMetaDataStore createMetaDataStore(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath metaDataPath,
                                               @Nullable Descriptors.FileDescriptor localFileDescriptor) {
    FDBMetaDataStore metaDataStore = new FDBMetaDataStore(context, metaDataPath);
    metaDataStore.setMaintainHistory(true);
    metaDataStore.setLocalFileDescriptor(localFileDescriptor);
    return metaDataStore;
  }
}
