/**
 * Copyright 2020 Pierre Zemb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    datasetsLoader.loadDataset("USER");
  }

  @ParameterizedTest
  @MethodSource("generateRequest")
  public void testQuery(RecordStoreProtocol.QueryRequest request, int expectedResult) {
    RecordQuery query = GrpcQueryGenerator.generate(request);

    List<Message> results = this.recordLayer.queryRecords(DatasetsLoader.DEFAULT_DEMO_TENANT, "USER", query);
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
        .setRecordTypeName("User")
        .build(), 100),

      // get on id
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
            .setField("id").setInt64Value(1)
            .setOperation(RecordStoreProtocol.FilterOperation.EQUALS)
            .build())
          .build())
        .build(), 1),

      // range
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setAndNode(RecordStoreProtocol.QueryFilterAndNode.newBuilder()
            .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
              .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
                .setField("id")
                .setInt64Value(1)
                .setOperation(RecordStoreProtocol.FilterOperation.GREATER_THAN_OR_EQUALS)
                .build())
              .build())
            .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
              .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
                .setInt64Value(10)
                .setField("id")
                .setOperation(RecordStoreProtocol.FilterOperation.LESS_THAN_OR_EQUALS)
                .build())
              .build())
            .build())
          .build())
        .build(), 10),

      // or
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setOrNode(RecordStoreProtocol.QueryFilterOrNode.newBuilder()
            .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
              .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
                .setField("id")
                .setInt64Value(1)
                .setOperation(RecordStoreProtocol.FilterOperation.EQUALS)
                .build())
              .build())
            .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
              .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
                .setInt64Value(10)
                .setField("id")
                .setOperation(RecordStoreProtocol.FilterOperation.EQUALS)
                .build())
              .build())
            .build())
          .build())
        .build(), 2),

      // text index any
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
            .setField("rick_and_morty_quotes")
            .setOperation(RecordStoreProtocol.FilterOperation.TEXT_CONTAINS_ANY)
            .addTokens("jerry")
            .build())
          .build())
        .build(), -1),

      // text index all
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
            .setField("rick_and_morty_quotes")
            .setOperation(RecordStoreProtocol.FilterOperation.TEXT_CONTAINS_ALL)
            .addAllTokens(Arrays.asList("MR MEESEEKS LOOK AT ME".toLowerCase().split(" ")))
            .build())
          .build())
        .build(), -1),

      // query over a repeated field
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
            .setField("beers")
            .setIsFieldDefinedAsRepeated(true)
            .setOperation(RecordStoreProtocol.FilterOperation.EQUALS)
            .setStringValue("Trappistes Rochefort 10")
            .build())
          .build())
        .build(), -1),

      // query over an indexed map with constraint on key and value
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setMapNode(RecordStoreProtocol.QueryFilterMapNode.newBuilder()
            .setField("favorite_locations_from_tv")
            .setKey(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
              .setOperation(RecordStoreProtocol.FilterOperation.START_WITH)
              .setStringValue("hitchhikers_guide")
              .build())
            .setValue(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
              .setStringValue("Eroticon VI")
              .setOperation(RecordStoreProtocol.FilterOperation.EQUALS)
              .build())
            .build())
          .build())
        .build(), -1),

      // query over an indexed map with constraint on value
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setMapNode(RecordStoreProtocol.QueryFilterMapNode.newBuilder()
            .setField("favorite_locations_from_tv")
            .setValue(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
              .setStringValue("Earth")
              .setOperation(RecordStoreProtocol.FilterOperation.EQUALS)
              .build())
            .build())
          .build())
        .build(), -1),

      // query over an indexed map with constraint on key
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setMapNode(RecordStoreProtocol.QueryFilterMapNode.newBuilder()
            .setField("favorite_locations_from_tv")
            .setKey(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
              .setStringValue("hitch")
              .setOperation(RecordStoreProtocol.FilterOperation.START_WITH)
              .build())
            .build())
          .build())
        .build(), 100),

      // query over an indexed nested field
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setRecordTypeName("User")
        .setFilter(RecordStoreProtocol.QueryFilterNode.newBuilder()
          .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
            .setField("address")
            .setOperation(RecordStoreProtocol.FilterOperation.MATCHES)
            .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
              .setField("city")
              .setOperation(RecordStoreProtocol.FilterOperation.EQUALS)
              .setStringValue("Antown")
              .build())
            .build())
          .build())
        .build(), -1)
    );
  }
}
