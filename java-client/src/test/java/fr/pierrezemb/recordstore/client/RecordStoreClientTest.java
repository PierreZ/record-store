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
package fr.pierrezemb.recordstore.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.Constants;
import fr.pierrezemb.recordstore.GrpcVerticle;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import fr.pierrezemb.recordstore.datasets.proto.DemoUserProto;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.AbstractFDBContainer;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RecordStoreClientTest extends AbstractFDBContainer {

  public final int port = PortManager.nextFreePort();
  private String sealedBiscuit;
  private RecordStoreClient recordStoreClient;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {

    File clusterFile = container.getClusterFile();

    DeploymentOptions options =
        new DeploymentOptions()
            .setConfig(
                new JsonObject()
                    .put(Constants.CONFIG_FDB_CLUSTER_FILE, clusterFile.getAbsolutePath())
                    .put(Constants.CONFIG_LOAD_DEMO, "USER")
                    .put(Constants.CONFIG_GRPC_LISTEN_PORT, port));

    BiscuitManager biscuitManager = new BiscuitManager();
    sealedBiscuit =
        biscuitManager.create(DatasetsLoader.DEFAULT_DEMO_TENANT, Collections.emptyList());

    // deploy verticle
    vertx.deployVerticle(
        new GrpcVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  @Order(1)
  public void testCreateClient(Vertx vertx, VertxTestContext testContext) throws Exception {

    recordStoreClient =
        new RecordStoreClient.Builder()
            .withRecordSpace(this.getClass().getName())
            .withTenant(DatasetsLoader.DEFAULT_DEMO_TENANT)
            .withToken(sealedBiscuit)
            .withAddress("localhost:" + port)
            .connect();

    recordStoreClient.ping().get();
    testContext.completeNow();
  }

  @Order(2)
  @RepeatedTest(3)
  public void testUploadSchema(Vertx vertx, VertxTestContext testContext)
      throws ExecutionException, InterruptedException {

    RecordStoreProtocol.UpsertSchemaRequest request =
        SchemaUtils.createSchemaRequest(
            DemoUserProto.User.getDescriptor(), // descriptor
            DemoUserProto.User.class.getSimpleName(), // name of the recordType
            "id", // primary key field
            "name", // index field
            RecordStoreProtocol.IndexType.VALUE // index type
            );

    recordStoreClient.upsertSchema(request).get();

    testContext.completeNow();
  }

  @Order(3)
  @RepeatedTest(3)
  public void testPut(Vertx vertx, VertxTestContext testContext)
      throws ExecutionException, InterruptedException {

    DemoUserProto.User record =
        DemoUserProto.User.newBuilder()
            .setId(999)
            .setName("Pierre Zemb")
            .setEmail("pz@example.org")
            .build();

    recordStoreClient.putRecord(record).get();

    testContext.completeNow();
  }

  @Test
  @Order(4)
  public void testGetStats(Vertx vertx, VertxTestContext testContext)
      throws ExecutionException, InterruptedException {

    RecordStoreProtocol.StatResponse stats = recordStoreClient.getStats().get();
    assertEquals("bad count of records", 1, stats.getCount());
    assertEquals("bad number of updates", 3, stats.getCountUpdates());

    testContext.completeNow();
  }

  @Test
  @Order(5)
  public void testQuery(Vertx vertx, VertxTestContext testContext)
      throws ExecutionException, InterruptedException, InvalidProtocolBufferException {

    RecordStoreProtocol.QueryRequest request =
        RecordStoreProtocol.QueryRequest.newBuilder()
            .setRecordTypeName(DemoUserProto.User.class.getSimpleName())
            .setFilter(RecordQuery.field("id").lessThan(1000L))
            .setResultLimit(1)
            .build();

    Iterator<RecordStoreProtocol.QueryResponse> results = recordStoreClient.queryRecords(request);

    assertTrue("bad length of results", results.hasNext());
    DemoUserProto.User response =
        DemoUserProto.User.parseFrom(results.next().getRecord().toByteArray());
    assertEquals("bad id", 999, response.getId());
    assertEquals("bad name", "Pierre Zemb", response.getName());
    assertEquals("bad mail", "pz@example.org", response.getEmail());

    testContext.completeNow();
  }
}
