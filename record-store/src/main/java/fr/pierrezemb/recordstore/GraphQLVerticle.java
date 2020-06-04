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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;

public class GraphQLVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphQLVerticle.class);
  private RecordLayer recordLayer;

  public static void main(String[] args) {
    Launcher.executeCommand("run", GraphQLVerticle.class.getName(), "-conf", "./config.json");
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    Integer port = this.context.config().getInteger("graphql-listen-port", 8081);
    GraphiQLHandlerOptions options = new GraphiQLHandlerOptions()
      .setQuery("{ allRecords { name } }")
      .setHeaders(ImmutableMap.of("tenant", "my-tenant", "container", "my-container"))
      .setEnabled(true);

    String clusterFilePath = this.context.config().getString("fdb-cluster-file", "/var/fdb/fdb.cluster");
    byte[] key = this.context.config().getString("encryption-key", GrpcVerticle.DEFAULT_ENCRYPTION_KEY).getBytes();
    SecretKeySpec secretKey = new SecretKeySpec(key, "AES");

    recordLayer = new RecordLayer(clusterFilePath, vertx.isMetricsEnabled(), secretKey);

    DatasetsLoader datasetsLoader = new DatasetsLoader(recordLayer);
    datasetsLoader.LoadDataset(this.context.config().getString("load-demo", "PERSONS"));

    Router router = Router.router(vertx);
    router.route("/api/v0/:tenant/:container/schema").handler(this::getSchema);
    router.route("/graphiql/*").handler(GraphiQLHandler.create(options));
    router.route("/graphql").handler(new RecordStoreGraphQLHandler(recordLayer));

    LOGGER.info("starting graphQL server on {}", port);

    vertx.createHttpServer()
      .requestHandler(router)
      .listen(port);

    startPromise.complete();
  }

  private void getSchema(RoutingContext routingContext) {
    String tenant = routingContext.request().getParam("tenant");
    if (tenant == null) {
      routingContext.response().setStatusCode(400).end();
    }
    String container = routingContext.request().getParam("container");
    if (container == null) {
      routingContext.response().setStatusCode(400).end();
    }

    try {
      RecordMetaData metadata = this.recordLayer.getSchema(tenant, container);
      String schema = GraphQLSchemaGenerator.generate(metadata);
      routingContext.response().putHeader("Content-Type", "text/plain").setStatusCode(200).end(schema);
    } catch (RuntimeException e) {
      LOGGER.error(e.getMessage());
      routingContext.response().setStatusCode(500).end(e.getMessage());
    }
  }
}
