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
package fr.pierrezemb.recordstore;

import static fr.pierrezemb.recordstore.datasets.DatasetsLoader.DEFAULT_DEMO_TENANT;
import static io.vertx.junit5.web.TestRequest.bodyResponse;
import static io.vertx.junit5.web.TestRequest.testRequest;

import com.apple.foundationdb.record.RecordMetaData;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.graphql.GraphQLSchemaGenerator;
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
import java.io.File;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.AbstractFDBContainer;

@ExtendWith({VertxExtension.class, VertxWebClientExtension.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GraphQLVerticleTest extends AbstractFDBContainer {
  public final int port = PortManager.nextFreePort();

  @WebClientOptionsInject
  public WebClientOptions opts =
      new WebClientOptions().setDefaultPort(port).setDefaultHost("localhost");

  private File clusterFile;
  private RecordLayer recordLayer;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext)
      throws InterruptedException, TimeoutException, ExecutionException {

    clusterFile = container.getClusterFile();
    SecretKeySpec secretKey = new SecretKeySpec(Constants.CONFIG_ENCRYPTION_KEY.getBytes(), "AES");
    recordLayer =
        new RecordLayer(clusterFile.getAbsolutePath(), vertx.isMetricsEnabled(), secretKey);

    DeploymentOptions options =
        new DeploymentOptions()
            .setConfig(
                new JsonObject()
                    .put(Constants.CONFIG_LOAD_DEMO, "User")
                    .put(Constants.CONFIG_FDB_CLUSTER_FILE, clusterFile.getAbsolutePath())
                    .put(Constants.CONFIG_GRAPHQL_LISTEN_PORT, port));

    BiscuitManager biscuitManager = new BiscuitManager();
    String sealedBiscuit =
        biscuitManager.create(DatasetsLoader.DEFAULT_DEMO_TENANT, Collections.emptyList());
    System.out.println(sealedBiscuit);

    // deploy verticle
    vertx.deployVerticle(
        new GraphQLVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  public void getSchema(WebClient client, VertxTestContext testContext) throws Exception {
    RecordMetaData metadata = this.recordLayer.getSchema(DEFAULT_DEMO_TENANT, "USER");
    String schema = GraphQLSchemaGenerator.generate(metadata);
    System.out.println(schema);
    testRequest(
            client,
            HttpMethod.GET,
            "/api/v0/" + DatasetsLoader.DEFAULT_DEMO_TENANT + "/" + "USER" + "/schema")
        .expect(bodyResponse(Buffer.buffer(schema), "text/plain"))
        .send(testContext);
  }

  @AfterAll
  public void afterAll(Vertx vertx, VertxTestContext testContext) throws Exception {
    vertx.close();
    testContext.completeNow();
  }
}
