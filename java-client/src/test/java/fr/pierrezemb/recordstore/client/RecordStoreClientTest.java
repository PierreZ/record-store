package fr.pierrezemb.recordstore.client;

import fr.pierrezemb.recordstore.Constants;
import fr.pierrezemb.recordstore.GrpcVerticle;
import fr.pierrezemb.recordstore.auth.BiscuitClientCredential;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.AbstractFDBContainer;

import java.io.File;
import java.util.Collections;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordStoreClientTest extends AbstractFDBContainer {

  public static final String DEFAULT_TENANT = "my-tenant";
  public final int port = PortManager.nextFreePort();
  private File clusterFile;
  private BiscuitClientCredential credentials;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {

    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put(Constants.CONFIG_FDB_CLUSTER_FILE, clusterFile.getAbsolutePath())
        .put(Constants.CONFIG_GRPC_LISTEN_PORT, port));

    BiscuitManager biscuitManager = new BiscuitManager();
    String sealedBiscuit = biscuitManager.create(DEFAULT_TENANT, Collections.emptyList());
    System.out.println(sealedBiscuit);
    credentials = new BiscuitClientCredential(DEFAULT_TENANT, sealedBiscuit, this.getClass().getName());

    // deploy verticle
    vertx.deployVerticle(new GrpcVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  public void testCreateClient(Vertx vertx, VertxTestContext testContext) throws Exception {

    RecordStoreClient recordStoreClient = new RecordStoreClient.Builder()
      .withContainer(this.getClass().getName())
      .withTenant(DEFAULT_TENANT)
      .withToken(credentials.toString())
      .withAddress("localhost:" + port)
      .build();

    testContext.completeNow();

  }
}
