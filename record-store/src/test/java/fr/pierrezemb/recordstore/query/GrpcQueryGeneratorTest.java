package fr.pierrezemb.recordstore.query;

import com.apple.foundationdb.record.query.RecordQuery;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import fr.pierrezemb.recordstore.AbstractFDBContainer;
import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

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

  @ParameterizedTest
  @MethodSource("generateRequest")
  public void testQuery(RecordStoreProtocol.QueryRequest request, int expectedResult) {
    RecordQuery query = GrpcQueryGenerator.generate(request);

    List<Message> results = this.recordLayer.queryRecords(DatasetsLoader.DEFAULT_DEMO_TENANT, "PERSONS", query);
    Assert.assertEquals("bad length of results", expectedResult, results.size());
  }

  private Stream<Arguments> generateRequest() {
    return Stream.of(
      Arguments.of(RecordStoreProtocol.QueryRequest.newBuilder()
        .setTable("Person")
        .build(), 100)
    );
  }
}
