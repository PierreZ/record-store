package fr.pierrezemb.recordstore.query;

import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import fr.pierrezemb.recordstore.AbstractFDBContainer;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GrpcQueryGeneratorTest extends AbstractFDBContainer {
  private File clusterFile;
  private RecordLayer recordLayer;

  @BeforeAll
  void beforeAll() throws IOException, InterruptedException, TimeoutException, ExecutionException, Descriptors.DescriptorValidationException {

    clusterFile = container.getClusterFile();
    recordLayer = new RecordLayer(clusterFile.getAbsolutePath(), false);

    DatasetsLoader datasetsLoader = new DatasetsLoader(recordLayer);
    datasetsLoader.LoadDataset("PERSONS");
  }

  @Test
  public void testRepeatedString() throws Exception {

    RecordQuery query = RecordQuery.newBuilder()
      .setRecordType("Person")
      .setFilter(Query.field("beers").oneOfThem().equalsValue("Trappistes Rochefort 8"))
      .build();

    List<Message> results = this.recordLayer.queryRecords(DatasetsLoader.DEFAULT_DEMO_TENANT, "PERSONS", query);
    System.out.println(results.size());
  }
}
