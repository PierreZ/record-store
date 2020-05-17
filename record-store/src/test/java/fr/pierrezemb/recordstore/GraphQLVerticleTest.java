package fr.pierrezemb.recordstore;

import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.junit5.web.VertxWebClientExtension;
import io.vertx.junit5.web.WebClientOptionsInject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static io.vertx.junit5.web.TestRequest.bodyResponse;
import static io.vertx.junit5.web.TestRequest.testRequest;

@ExtendWith({
  VertxExtension.class,
  VertxWebClientExtension.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GraphQLVerticleTest {
  public final int port = PortManager.nextFreePort();
  private final FoundationDBContainer container = new FoundationDBContainer();
  @WebClientOptionsInject
  public WebClientOptions opts = new WebClientOptions()
    .setDefaultPort(port)
    .setDefaultHost("localhost");
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
  public void getSchema(WebClient client, VertxTestContext testContext) throws Exception {
    // Thread.sleep(99999999999L);
    testRequest(client, HttpMethod.GET, "/api/v0/" + DatasetsLoader.DEFAULT_DEMO_TENANT + "/" + "PERSONS" + "/schema")
      .expect(
        bodyResponse(Buffer.buffer("type Person {\n" +
          "  email: String\n" +
          "  id: Long\n" +
          "  name: String\n" +
          "}\n" +
          "\n" +
          ""), "text/plain")
      ).send(testContext);
  }
}
