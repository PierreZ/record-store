package fr.pierrezemb.recordstore;


import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import fr.pierrezemb.recordstore.grpc.AuthInterceptor;
import fr.pierrezemb.recordstore.grpc.RecordService;
import fr.pierrezemb.recordstore.grpc.SchemaService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.grpc.VertxServer;
import io.vertx.grpc.VertxServerBuilder;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    String clusterFilePath = this.context.config().getString("fdb-cluster-file", "/var/fdb/fdb.cluster");
    System.out.println("connecting to fdb@" + clusterFilePath);

    FDBDatabase db = FDBDatabaseFactory.instance().getDatabase(clusterFilePath);

    VertxServerBuilder serverBuilder = VertxServerBuilder
      .forAddress(vertx,
        this.context.config().getString("listen-address", "localhost"),
        this.context.config().getInteger("listen-port", 8080))
      .intercept(new AuthInterceptor())
      .addService(new SchemaService(db))
      .addService(new RecordService(db));

    VertxServer server = serverBuilder.build();

    server.start(ar -> {
      if (ar.succeeded()) {
        System.out.println("gRPC service started");
        startPromise.complete();
      } else {
        System.out.println("Could not start server " + ar.cause().getMessage());
        startPromise.fail(ar.cause());
      }
    });
  }
}
