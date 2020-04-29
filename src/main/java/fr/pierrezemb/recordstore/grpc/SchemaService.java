package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBMetaDataStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import fr.pierrezemb.recordstore.fdb.RSMetaDataStore;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import fr.pierrezemb.recordstore.utils.ProtobufReflectionUtil;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaService extends SchemaServiceGrpc.SchemaServiceImplBase {
  // Keep a global track of the number of records stored
  protected static final Index COUNT_INDEX = new Index(
    "globalRecordCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
  private final MetaDataEvolutionValidator validator = MetaDataEvolutionValidator.getDefaultInstance();
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
    String container = GrpcContextKeys.getContainerOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID, container);

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

      // add user indexes
      for (RecordStoreProtocol.IndexDefinition indexDefinition : request.getIndexDefinitionsList()) {
        metadataBuilder.addIndex(
          request.getName(),
          request.getName() + "_idx_" + indexDefinition.getField() + "_" + indexDefinition.getIndexType().toString(),
          Key.Expressions.field(indexDefinition.getField()));
      }

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

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void list(RecordStoreProtocol.ListSchemaRequest request, StreamObserver<RecordStoreProtocol.ListSchemaResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String container = GrpcContextKeys.getContainerOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID, container);
      List<RecordStoreProtocol.SchemaDescription> records = metaDataStore.getRecordMetaData()
        .getRecordTypes()
        .entrySet()
        .stream()
        .map(e -> RecordStoreProtocol.SchemaDescription.newBuilder()
          .setName(e.getKey())
          .setPrimaryKeyField(e.getValue().getPrimaryKey().toKeyExpression().getField().getFieldName())
          .setSchema(RecordStoreProtocol.SelfDescribedMessage.newBuilder()
            .setDescriptorSet(ProtobufReflectionUtil.protoFileDescriptorSet(e.getValue().getDescriptor()))
            .build())
          .build())
        .collect(Collectors.toList());

      responseObserver.onNext(RecordStoreProtocol.ListSchemaResponse.newBuilder()
        .addAllSchemas(records)
        .build());
      responseObserver.onCompleted();
    }
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void get(RecordStoreProtocol.GetSchemaRequest request, StreamObserver<RecordStoreProtocol.GetSchemaResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String container = GrpcContextKeys.getContainerOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID, container);

      List<RecordStoreProtocol.SchemaDescription> records =
        ImmutableMap.of(request.getTable(), metaDataStore.getRecordMetaData().getRecordType(request.getTable()))
          .entrySet()
          .stream()
          .map(e -> RecordStoreProtocol.SchemaDescription.newBuilder()
            .setName(e.getKey())
            .setPrimaryKeyField(e.getValue().getPrimaryKey().toKeyExpression().getField().getFieldName())
            .setSchema(RecordStoreProtocol.SelfDescribedMessage.newBuilder()
              .setDescriptorSet(ProtobufReflectionUtil.protoFileDescriptorSet(e.getValue().getDescriptor()))
              .build())
            .build())
          .collect(Collectors.toList());

      responseObserver.onNext(RecordStoreProtocol.GetSchemaResponse.newBuilder()
        .setSchemas(records.get(0))
        .build());
      responseObserver.onCompleted();
    }
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void addIndex(RecordStoreProtocol.AddIndexRequest request, StreamObserver<RecordStoreProtocol.AddIndexResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String container = GrpcContextKeys.getContainerOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID, container);
      for (RecordStoreProtocol.IndexDefinition indexDefinition : request.getIndexDefinitionsList()) {
        metaDataStore.addIndex(
          request.getTable(),
          request.getTable() + "_idx_" + indexDefinition.getField() + "_" + indexDefinition.getIndexType().toString(),
          Key.Expressions.field(indexDefinition.getField()));
      }
    }
    responseObserver.onNext(RecordStoreProtocol.AddIndexResponse.newBuilder()
      .setResult(RecordStoreProtocol.Result.OK).build());
    responseObserver.onCompleted();
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void upgradeSchema(RecordStoreProtocol.UpgradeSchemaRequest request, StreamObserver<RecordStoreProtocol.UpgradeSchemaResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String container = GrpcContextKeys.getContainerOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID, container);

      // retrieving protobuf descriptor
      RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder();
      DescriptorProtos.FileDescriptorSet descriptorSet = request.getSchema().getDescriptorSet();
      for (DescriptorProtos.FileDescriptorProto fdp : descriptorSet.getFileList()) {
        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(fdp, new Descriptors.FileDescriptor[]{});
        // updating schema
        metadataBuilder.updateRecords(fd);
      }

      // add new indexes
      for (RecordStoreProtocol.IndexDefinition indexDefinition : request.getIndexDefinitionsList()) {
        metadataBuilder.addIndex(
          request.getName(),
          request.getName() + "_idx_" + indexDefinition.getField() + "_" + indexDefinition.getIndexType().toString(),
          Key.Expressions.field(indexDefinition.getField()));
      }

      // and save it
      metaDataStore.saveRecordMetaData(metadataBuilder.getRecordMetaData().toProto());
      context.commit();

    } catch (Descriptors.DescriptorValidationException e) {
      responseObserver.onError(e);
      responseObserver.onCompleted();
    }

    responseObserver.onNext(RecordStoreProtocol.UpgradeSchemaResponse.newBuilder().setResult(RecordStoreProtocol.Result.OK).build());
    responseObserver.onCompleted();
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void delete(RecordStoreProtocol.DeleteSchemaRequest request, StreamObserver<RecordStoreProtocol.DeleteSchemaResponse> responseObserver) {
    super.delete(request, responseObserver);
  }
}
