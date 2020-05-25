package fr.pierrezemb.recordstore.query;

import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.google.protobuf.Message;
import fr.pierrezemb.recordstore.FoundationDBContainer;
import fr.pierrezemb.recordstore.GraphQLVerticle;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GrpcQueryGeneratorTest {
  private final FoundationDBContainer container = new FoundationDBContainer();
  private File clusterFile;
  private RecordLayer recordLayer;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException, TimeoutException, ExecutionException {

    container.start();
    clusterFile = container.getClusterFile();
    recordLayer = new RecordLayer(clusterFile.getAbsolutePath(), vertx.isMetricsEnabled());

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put("fdb-cluster-file", clusterFile.getAbsolutePath()));

    BiscuitManager biscuitManager = new BiscuitManager();
    String sealedBiscuit = biscuitManager.create(DatasetsLoader.DEFAULT_DEMO_TENANT, Collections.emptyList());
    System.out.println(sealedBiscuit);

    // deploy verticle
    vertx.deployVerticle(new GraphQLVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
  }

  @Test
  public void testRepeatedString(Vertx vertx, VertxTestContext testContext) throws Exception {

    RecordQuery query = RecordQuery.newBuilder()
      .setRecordType("Person")
      .setFilter(Query.field("beers").oneOfThem().equalsValue("Trappistes Rochefort 8"))
      .build();

    List<Message> results = this.recordLayer.queryRecords(DatasetsLoader.DEFAULT_DEMO_TENANT, "PERSONS", query);
    if (results.size() > 0) {
      testContext.completeNow();
    } else {
      testContext.failed();
    }
  }
}
