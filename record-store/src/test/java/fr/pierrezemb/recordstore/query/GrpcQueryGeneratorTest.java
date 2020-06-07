package fr.pierrezemb.recordstore.query;

import com.apple.foundationdb.record.query.RecordQuery;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import fr.pierrezemb.recordstore.Constants;
import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.AbstractFDBContainer;

import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
/**
 * GrpcQueryGeneratorTest is using the ./record-store/src/main/proto/demo_person.proto
 * protobuf to test some queries
 */
class GrpcQueryGeneratorTest extends AbstractFDBContainer {
  private File clusterFile;
  private RecordLayer recordLayer;

  @BeforeAll
  void beforeAll() throws IOException, InterruptedException, TimeoutException, ExecutionException, Descriptors.DescriptorValidationException {

    clusterFile = container.getClusterFile();
    SecretKeySpec secretKey = new SecretKeySpec(Constants.CONFIG_ENCRYPTION_KEY_DEFAULT.getBytes(), "AES");
    recordLayer = new RecordLayer(clusterFile.getAbsolutePath(), false, secretKey);

    DatasetsLoader datasetsLoader = new DatasetsLoader(recordLayer);
    datasetsLoader.LoadDataset("PERSONS");
  }

  @ParameterizedTest
  @MethodSource("generateRequest")
  public void testQuery(RecordStoreProtocol.QueryRequest request, int expectedResult) {
    RecordQuery query = GrpcQueryGenerator.generate(request);

    List<Message> results = this.recordLayer.queryRecords(DatasetsLoader.DEFAULT_DEMO_TENANT, "PERSONS", query);
    if (expectedResult == -1) {
      Assert.assertTrue("empty results", results.size() > 0);
    } else {
      Assert.assertEquals("bad length of results", expectedResult, results.size());
    }
  }

  private Stream<Arguments> generateRequest() {
    return Stream.of(
      // all records
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .build(), 100),

      // get on id
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
            .setField("id").setInt64Value(1)
            .setOperation(RecordStoreProtocol.FieldOperation.EQUALS)
            .build())
          .build())
        .build(), 1),

      // range
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setAndNode(RecordStoreProtocol.AndNode.newBuilder()
            .addNodes(RecordStoreProtocol.Node.newBuilder()
              .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
                .setField("id")
                .setInt64Value(1)
                .setOperation(RecordStoreProtocol.FieldOperation.GREATER_THAN_OR_EQUALS)
                .build())
              .build())
            .addNodes(RecordStoreProtocol.Node.newBuilder()
              .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
                .setInt64Value(10)
                .setField("id")
                .setOperation(RecordStoreProtocol.FieldOperation.LESS_THAN_OR_EQUALS)
                .build())
              .build())
            .build())
          .build())
        .build(), 10),

      // or
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setOrNode(RecordStoreProtocol.OrNode.newBuilder()
            .addNodes(RecordStoreProtocol.Node.newBuilder()
              .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
                .setField("id")
                .setInt64Value(1)
                .setOperation(RecordStoreProtocol.FieldOperation.EQUALS)
                .build())
              .build())
            .addNodes(RecordStoreProtocol.Node.newBuilder()
              .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
                .setInt64Value(10)
                .setField("id")
                .setOperation(RecordStoreProtocol.FieldOperation.EQUALS)
                .build())
              .build())
            .build())
          .build())
        .build(), 2),

      // text index any
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
            .setField("rick_and_morty_quotes")
            .setOperation(RecordStoreProtocol.FieldOperation.TEXT_CONTAINS_ANY)
            .addTokens("jerry")
            .build())
          .build())
        .build(), -1),

      // text index all
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
            .setField("rick_and_morty_quotes")
            .setOperation(RecordStoreProtocol.FieldOperation.TEXT_CONTAINS_ALL)
            .addAllTokens(Arrays.asList("MR MEESEEKS LOOK AT ME".toLowerCase().split(" ")))
            .build())
          .build())
        .build(), -1),

      // query over a repeated field
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
            .setField("beers")
            .setIsFieldDefinedAsRepeated(true)
            .setOperation(RecordStoreProtocol.FieldOperation.EQUALS)
            .setStringValue("Trappistes Rochefort 10")
            .build())
          .build())
        .build(), -1),

      // query over an indexed map with constraint on key and value
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setMapNode(RecordStoreProtocol.MapNode.newBuilder()
            .setField("favorite_locations_from_tv")
            .setKey(RecordStoreProtocol.FieldNode.newBuilder()
              .setOperation(RecordStoreProtocol.FieldOperation.START_WITH)
              .setStringValue("hitchhikers_guide")
              .build())
            .setValue(RecordStoreProtocol.FieldNode.newBuilder()
              .setStringValue("Eroticon VI")
              .setOperation(RecordStoreProtocol.FieldOperation.EQUALS)
              .build())
            .build())
          .build())
        .build(), -1),

      // query over an indexed map with constraint on value
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setMapNode(RecordStoreProtocol.MapNode.newBuilder()
            .setField("favorite_locations_from_tv")
            .setValue(RecordStoreProtocol.FieldNode.newBuilder()
              .setStringValue("Earth")
              .setOperation(RecordStoreProtocol.FieldOperation.EQUALS)
              .build())
            .build())
          .build())
        .build(), -1),

      // query over an indexed map with constraint on key
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setMapNode(RecordStoreProtocol.MapNode.newBuilder()
            .setField("favorite_locations_from_tv")
            .setKey(RecordStoreProtocol.FieldNode.newBuilder()
              .setStringValue("hitch")
              .setOperation(RecordStoreProtocol.FieldOperation.START_WITH)
              .build())
            .build())
          .build())
        .build(), 100),

      // query over an indexed nested field
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .setQueryNode(RecordStoreProtocol.Node.newBuilder()
          .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
            .setField("address")
            .setOperation(RecordStoreProtocol.FieldOperation.MATCHES)
            .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
              .setField("city")
              .setOperation(RecordStoreProtocol.FieldOperation.EQUALS)
              .setStringValue("Antown")
              .build())
            .build())
          .build())
        .build(), -1)
    );
  }
}
