package fr.pierrezemb.recordstore.graphql;

import com.apple.foundationdb.record.RecordMetaData;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.AbstractFDBContainer;
import fr.pierrezemb.recordstore.Constants;
import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static fr.pierrezemb.recordstore.datasets.DatasetsLoader.DEFAULT_DEMO_TENANT;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GraphQLSchemaGeneratorTest extends AbstractFDBContainer {
  private File clusterFile;
  private RecordLayer recordLayer;

  @BeforeAll
  void setUp() throws InterruptedException, ExecutionException, TimeoutException, InvalidProtocolBufferException, Descriptors.DescriptorValidationException {
    clusterFile = container.getClusterFile();

    SecretKeySpec secretKey = new SecretKeySpec(Constants.CONFIG_ENCRYPTION_KEY_DEFAULT.getBytes(), "AES");
    recordLayer = new RecordLayer(clusterFile.getAbsolutePath(), false, secretKey);

    DatasetsLoader datasetsLoader = new DatasetsLoader(recordLayer);
    datasetsLoader.LoadDataset("PERSONS");
  }

  @Test
  void generate() {
    RecordMetaData metadata = this.recordLayer.getSchema(DEFAULT_DEMO_TENANT, "PERSONS");
    String schema = GraphQLSchemaGenerator.generate(metadata);
    System.out.println(schema);

    ImmutableList<String> shouldContains = ImmutableList.of(
      "type Person",
      "email: String",
      "id: Long",
      "type Query {",
      "allPersons(limit: Int): [Person!]!",
      "getPersonByEmail(email: String): Person!",
      "getPersonByName(name: String): Person!"
    );
    for (String shouldContain : shouldContains) {
      assertTrue(schema.contains(shouldContain), "schema does not contain '" + shouldContain + "'");
    }
  }
}
