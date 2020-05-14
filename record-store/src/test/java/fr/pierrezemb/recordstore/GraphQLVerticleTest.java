package fr.pierrezemb.recordstore;

import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GraphQLVerticleTest {
  public final int port = PortManager.nextFreePort();
  private final FoundationDBContainer container = new FoundationDBContainer();
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    container.start();
    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put("fdb-cluster-file", clusterFile.getAbsolutePath())
        .put("graphql-listen-port", port));

    BiscuitManager biscuitManager = new BiscuitManager();
    String sealedBiscuit = biscuitManager.create(DatasetsLoader.DEFAULT_DEMO_TENANT, Collections.emptyList());
    System.out.println(sealedBiscuit);

    // deploy verticle
    vertx.deployVerticle(new GraphQLVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  public void getSchema(Vertx vertx, VertxTestContext testContext) throws Exception {
    testContext.completeNow();
  }
}
