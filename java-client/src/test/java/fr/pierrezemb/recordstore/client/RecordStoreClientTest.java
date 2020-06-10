package fr.pierrezemb.recordstore.client;

import fr.pierrezemb.recordstore.Constants;
import fr.pierrezemb.recordstore.GrpcVerticle;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.proto.DemoPersonProto;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.AbstractFDBContainer;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RecordStoreClientTest extends AbstractFDBContainer {

  public static final String DEFAULT_TENANT = "my-tenant";
  public final int port = PortManager.nextFreePort();
  private String sealedBiscuit;
  private RecordStoreClient recordStoreClient;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {

    File clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put(Constants.CONFIG_FDB_CLUSTER_FILE, clusterFile.getAbsolutePath())
        .put(Constants.CONFIG_GRPC_LISTEN_PORT, port));

    BiscuitManager biscuitManager = new BiscuitManager();
    sealedBiscuit = biscuitManager.create(DEFAULT_TENANT, Collections.emptyList());

    // deploy verticle
    vertx.deployVerticle(new GrpcVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  @Order(1)
  public void testCreateClient(Vertx vertx, VertxTestContext testContext) throws Exception {

    recordStoreClient = new RecordStoreClient.Builder()
      .withContainer(this.getClass().getName())
      .withTenant(DEFAULT_TENANT)
      .withToken(sealedBiscuit)
      .withAddress("localhost:" + port)
      .build();

    recordStoreClient.ping().get();
    testContext.completeNow();
  }

  @Order(2)
  @RepeatedTest(3)
  public void testUploadSchema(Vertx vertx, VertxTestContext testContext) throws ExecutionException, InterruptedException {

    RecordStoreProtocol.UpsertSchemaRequest request = SchemaUtils.createSchemaRequest(
      DemoPersonProto.Person.getDescriptor(), // descriptor
      DemoPersonProto.Person.class.getSimpleName(), // name of the recordType
      "id", // primary key field
      "name", // index field
      RecordStoreProtocol.IndexType.VALUE // index type
    );

    recordStoreClient.upsertSchema(request).get();

    testContext.completeNow();
  }

  @Order(3)
  @RepeatedTest(3)
  public void testPut(Vertx vertx, VertxTestContext testContext) throws ExecutionException, InterruptedException {

    DemoPersonProto.Person record = DemoPersonProto.Person.newBuilder()
      .setId(1)
      .setName("Pierre Zemb")
      .setEmail("pz@example.org")
      .build();

    recordStoreClient.putRecord(record).get();

    testContext.completeNow();
  }
}
