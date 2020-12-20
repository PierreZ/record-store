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

import com.apple.foundationdb.record.RecordMetaData;
import com.google.common.collect.ImmutableMap;
import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.graphql.GraphQLSchemaGenerator;
import fr.pierrezemb.recordstore.graphql.RecordStoreGraphQLHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphQLVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphQLVerticle.class);
  private RecordLayer recordLayer;

  public static void main(String[] args) {
    Launcher.executeCommand("run", GraphQLVerticle.class.getName(), "-conf", "./config.json");
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    Integer port = this.context.config().getInteger(Constants.CONFIG_GRAPHQL_LISTEN_PORT, 8081);
    GraphiQLHandlerOptions options =
        new GraphiQLHandlerOptions()
            .setQuery("{ allRecords { name } }")
            .setHeaders(ImmutableMap.of("tenant", "my-tenant", "recordSpace", "my-recordSpace"))
            .setEnabled(true);

    String clusterFilePath =
        this.context
            .config()
            .getString(
                Constants.CONFIG_FDB_CLUSTER_FILE, Constants.CONFIG_FDB_CLUSTER_FILE_DEFAULT);
    byte[] key =
        this.context
            .config()
            .getString(
                Constants.CONFIG_ENCRYPTION_KEY_DEFAULT, Constants.CONFIG_ENCRYPTION_KEY_DEFAULT)
            .getBytes();
    SecretKeySpec secretKey = new SecretKeySpec(key, "AES");

    recordLayer = new RecordLayer(clusterFilePath, vertx.isMetricsEnabled(), secretKey);

    DatasetsLoader datasetsLoader = new DatasetsLoader(recordLayer);
    datasetsLoader.loadDataset(this.context.config().getString(Constants.CONFIG_LOAD_DEMO, ""));

    Router router = Router.router(vertx);
    router.route("/api/v0/:tenant/:recordspace/schema").handler(this::getSchema);
    router.route("/graphiql/*").handler(GraphiQLHandler.create(options));
    router.route("/graphql").handler(new RecordStoreGraphQLHandler(recordLayer));

    LOGGER.info("starting graphQL server on {}", port);

    vertx.createHttpServer().requestHandler(router).listen(port);

    startPromise.complete();
  }

  private void getSchema(RoutingContext routingContext) {
    String tenant = routingContext.request().getParam("tenant");
    if (tenant == null) {
      routingContext.response().setStatusCode(400).end();
    }
    String recordSpace = routingContext.request().getParam("recordspace");
    if (recordSpace == null) {
      routingContext.response().setStatusCode(400).end();
    }

    try {
      RecordMetaData metadata = this.recordLayer.getSchema(tenant, recordSpace);
      String schema = GraphQLSchemaGenerator.generate(metadata);
      routingContext
          .response()
          .putHeader("Content-Type", "text/plain")
          .setStatusCode(200)
          .end(schema);
    } catch (RuntimeException e) {
      LOGGER.error(e.getMessage());
      routingContext.response().setStatusCode(500).end(e.getMessage());
    }
  }
}
