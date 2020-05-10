package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.*;
import com.apple.foundationdb.record.metadata.*;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.*;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import fr.pierrezemb.recordstore.fdb.RecordStoreKeySpace;
import fr.pierrezemb.recordstore.fdb.RecordStoreMetaDataStore;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import fr.pierrezemb.recordstore.utils.ProtobufReflectionUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static fr.pierrezemb.recordstore.fdb.UniversalIndexes.*;

public class SchemaService extends SchemaServiceGrpc.SchemaServiceImplBase {
  private static final Logger log = LoggerFactory.getLogger(SchemaService.class);
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
  public void get(RecordStoreProtocol.GetSchemaRequest request, StreamObserver<RecordStoreProtocol.GetSchemaResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String container = GrpcContextKeys.getContainerOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      FDBMetaDataStore metaDataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);

      List<RecordStoreProtocol.IndexDescription> indexes = metaDataStore.getRecordMetaData().getAllIndexes().stream()
        .filter(e -> !e.getName().startsWith("global"))
        .map(e ->
          RecordStoreProtocol.IndexDescription.newBuilder()
            .build()
        ).collect(Collectors.toList());

      List<RecordStoreProtocol.SchemaDescription> records =
        ImmutableMap.of(request.getTable(), metaDataStore.getRecordMetaData().getRecordType(request.getTable()))
          .entrySet()
          .stream()
          .map(e -> RecordStoreProtocol.SchemaDescription.newBuilder()
            .setName(e.getKey())
            .addAllIndexes(indexes)
            .setPrimaryKeyField(e.getValue().getPrimaryKey().toKeyExpression().getField().getFieldName())
            .setSchema(ProtobufReflectionUtil.protoFileDescriptorSet(e.getValue().getDescriptor()))
            .build())
          .collect(Collectors.toList());


      responseObserver.onNext(RecordStoreProtocol.GetSchemaResponse.newBuilder()
        .setSchemas(records.get(0))
        .setVersion(metaDataStore.getRecordMetaData().getVersion())
        .build());
      responseObserver.onCompleted();

    } catch (RuntimeException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
  }


  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void upsert(RecordStoreProtocol.UpsertSchemaRequest request, StreamObserver<RecordStoreProtocol.EmptyResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String container = GrpcContextKeys.getContainerOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      FDBMetaDataStore metaDataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);

      RecordMetaData oldMetaData = null;
      int version = 0;
      try {
        oldMetaData = metaDataStore.getRecordMetaData();
        log.debug("metadata for {}:{} is in version {}", tenantID, container, oldMetaData.getVersion());
        version = oldMetaData.getVersion() + 1;
      } catch (FDBMetaDataStore.MissingMetaDataException e) {
        log.info("missing metadata, creating one");
      }

      RecordMetaData newRecordMetaData = createRecordMetaData(request, version, oldMetaData);

      // handling upgrade
      if (null != oldMetaData) {
        MetaDataEvolutionValidator metaDataEvolutionValidator = MetaDataEvolutionValidator.newBuilder()
          .setAllowIndexRebuilds(true)
          .setAllowMissingFormerIndexNames(false)
          .build();

        metaDataEvolutionValidator.validate(oldMetaData, newRecordMetaData);
      }

      // and save it
      metaDataStore.saveRecordMetaData(newRecordMetaData.getRecordMetaData().toProto());

      context.commit();

    } catch (Descriptors.DescriptorValidationException | MetaDataException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }

    responseObserver.onNext(RecordStoreProtocol.EmptyResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  private RecordMetaData createRecordMetaData(RecordStoreProtocol.UpsertSchemaRequest request, int version, RecordMetaData oldMetadata) throws Descriptors.DescriptorValidationException {

    // retrieving protobuf descriptor
    RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder();

    DescriptorProtos.FileDescriptorSet descriptorSet = request.getSchema();
    for (DescriptorProtos.FileDescriptorProto fdp : descriptorSet.getFileList()) {
      Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(fdp, new Descriptors.FileDescriptor[]{});
      // updating schema
      metadataBuilder.setRecords(fd);
    }

    // set options
    metadataBuilder.setVersion(version);
    metadataBuilder.setStoreRecordVersions(true);
    metadataBuilder.setSplitLongRecords(true);

    HashSet<Index> oldIndexes = oldMetadata != null ?
      new HashSet<>(oldMetadata.getAllIndexes()) :
      new HashSet<>();
    HashSet<String> oldIndexesNames = new HashSet<>();

    // add old indexes
    for (Index index : oldIndexes) {
      log.trace("adding old index {}", index.getName());
      oldIndexesNames.add(index.getName());
      metadataBuilder.addIndex(request.getName(), index);
    }

    // add new indexes
    for (RecordStoreProtocol.IndexDefinition indexDefinition : request.getIndexDefinitionsList()) {
      String indexName = request.getName() + "_idx_" + indexDefinition.getField() + "_" + indexDefinition.getIndexType().toString();
      if (!oldIndexesNames.contains(indexName)) {
        log.trace("adding new index {} of type {}", indexName, indexDefinition.getIndexType());
        Index index = null;
        switch (indexDefinition.getIndexType()) {
          case VALUE:
            index = new Index(
              indexName,
              Key.Expressions.field(indexDefinition.getField()),
              IndexTypes.VALUE);
            break;
          // https://github.com/FoundationDB/fdb-record-layer/blob/e70d3f9b5cec1cf37b6f540d4e673059f2a628ab/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/TextIndexMaintainer.java#L81-L93
          case TEXT_DEFAULT_TOKENIZER:
            index = new Index(
              indexName,
              Key.Expressions.field(indexDefinition.getField()),
              IndexTypes.TEXT);
            break;
          case VERSION:
            index = new Index(
              indexName,
              VersionKeyExpression.VERSION,
              IndexTypes.VERSION);
            break;
          case UNRECOGNIZED:
            continue;
        }
        metadataBuilder.addIndex(request.getName(), index);
      }
    }

    if (oldMetadata == null) {
      metadataBuilder.addUniversalIndex(COUNT_INDEX);
      metadataBuilder.addUniversalIndex(COUNT_UPDATES_INDEX);
    }

    // set primary key
    metadataBuilder.getRecordType(request.getName())
      .setPrimaryKey(buildPrimaryKeyExpression(request.getPrimaryKeyFieldsList().asByteStringList()));

    return metadataBuilder.build();
  }

  private KeyExpression buildPrimaryKeyExpression(List<ByteString> primaryKeyFields) {
    if (primaryKeyFields.size() == 1) {
      return Key.Expressions.field(primaryKeyFields.get(0).toStringUtf8());
    }

    return Key.Expressions.concat(
      primaryKeyFields
        .stream()
        .map(e -> Key.Expressions.field(e.toStringUtf8()))
        .collect(Collectors.toList())
    );
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void stat(RecordStoreProtocol.StatRequest request, StreamObserver<RecordStoreProtocol.StatResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String container = GrpcContextKeys.getContainerOrFail();


    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      // create recordStoreProvider
      FDBMetaDataStore metaDataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);

      // Helper func
      Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
        .setMetaDataProvider(metaDataStore)
        .setContext(context)
        .setKeySpacePath(RecordStoreKeySpace.getDataKeySpacePath(tenantID, container))
        .createOrOpen();

      FDBRecordStore r = recordStoreProvider.apply(context);

      CompletableFuture<Tuple> countFuture = r.evaluateAggregateFunction(
        EvaluationContext.EMPTY,
        Collections.emptyList(),
        INDEX_COUNT_AGGREGATE_FUNCTION,
        TupleRange.ALL,
        IsolationLevel.SERIALIZABLE);

      CompletableFuture<Tuple> updateFuture = r.evaluateAggregateFunction(
        EvaluationContext.EMPTY,
        Collections.emptyList(),
        INDEX_COUNT_UPDATES_AGGREGATE_FUNCTION,
        TupleRange.ALL,
        IsolationLevel.SERIALIZABLE);

      Tuple result = countFuture.thenCombine(updateFuture, (count, update)
        -> Tuple.from(count.getLong(0), update.getLong(0))).join();

      responseObserver.onNext(RecordStoreProtocol.StatResponse.newBuilder()
        .setCount(result.getLong(0))
        .setCountUpdates(result.getLong(1))
        .build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
  }
}
