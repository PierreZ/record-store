package fr.pierrezemb.recordstore;

import com.google.common.collect.ImmutableMap;
import fr.pierrezemb.recordstore.graphql.RecordStoreGraphQLHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphQLVerticle extends AbstractVerticle {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphQLVerticle.class);

  public static void main(String[] args) {
    Launcher.executeCommand("run", GraphQLVerticle.class.getName(), "-conf", "./config.json");
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    Integer port = this.context.config().getInteger("graphql-listen-port", 8081);
    GraphiQLHandlerOptions options = new GraphiQLHandlerOptions()
      .setHeaders(ImmutableMap.of("tenant", "my-tenant", "container", "my-container"))
      .setEnabled(true);

    Router router = Router.router(vertx);
    router.route("/graphiql/*").handler(GraphiQLHandler.create(options));
    router.route("/graphql").handler(new RecordStoreGraphQLHandler());

    LOGGER.info("starting graphQL server on {}", port);

    vertx.createHttpServer()
      .requestHandler(router)
      .listen(port);
  }
}
