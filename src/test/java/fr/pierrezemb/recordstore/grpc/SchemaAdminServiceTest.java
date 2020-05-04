package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.DescriptorProtos;
import fr.pierrezemb.recordstore.FoundationDBContainer;
import fr.pierrezemb.recordstore.MainVerticle;
import fr.pierrezemb.recordstore.PortManager;
import fr.pierrezemb.recordstore.auth.BiscuitClientCredential;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.proto.AdminServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocolTest;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import fr.pierrezemb.recordstore.utils.ProtobufReflectionUtil;
import io.grpc.ManagedChannel;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.grpc.VertxChannelBuilder;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.json.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;

import static fr.pierrezemb.recordstore.MainVerticleTest.DEFAULT_CONTAINER;
import static fr.pierrezemb.recordstore.MainVerticleTest.DEFAULT_TENANT;
import static org.junit.Assert.*;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SchemaAdminServiceTest {

  public final int port = PortManager.nextFreePort();
  private FoundationDBContainer container;
  private SchemaServiceGrpc.SchemaServiceVertxStub schemaServiceVertxStub;
  private AdminServiceGrpc.AdminServiceVertxStub adminServiceVertxStub;

  private static String OS = System.getProperty("os.name").toLowerCase();
  private File clusterFile;


  @BeforeAll
  void init() throws Exception{
    if (PortManager.listeningPort(FoundationDBContainer.FDB_PORT)) {
      System.out.println("Fdb: already reachable");
      if (OS.indexOf("nux") >= 0) {
        clusterFile = new File("/etc/foundationdb/fdb.cluster");
      } else if (OS.indexOf("mac") >= 0) {
        clusterFile = new File("/usr/local/etc/foundationdb/fdb.cluster");
      } else {
        System.out.println("Fdb: Can't get default clusterFile");
      }
    } else {
      System.out.println("Fdb not reachable, spawning container");
      container = new FoundationDBContainer(FoundationDBContainer.FDB_PORT);
      container.start();
      clusterFile = container.getClusterFile();
    }
  }


  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put("fdb-cluster-file", clusterFile.getAbsolutePath())
        .put("listen-port", port));

    BiscuitManager biscuitManager = new BiscuitManager();
    String sealedBiscuit = biscuitManager.create(DEFAULT_TENANT, Collections.emptyList());
    BiscuitClientCredential credentials = new BiscuitClientCredential(DEFAULT_TENANT, sealedBiscuit, DEFAULT_CONTAINER);

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
    ManagedChannel channel = VertxChannelBuilder
      .forAddress(vertx, "localhost", port)
      .usePlaintext(true)
      .build();

    schemaServiceVertxStub = SchemaServiceGrpc.newVertxStub(channel).withCallCredentials(credentials);
    adminServiceVertxStub = AdminServiceGrpc.newVertxStub(channel).withCallCredentials(credentials);
  }

  @BeforeAll
  public void testFdbReachable()
  {
    assertTrue(PortManager.listeningPort(FoundationDBContainer.FDB_PORT));

    ByteBuffer buffer = ByteBuffer.allocate("/status/json".length() + 2);
    buffer.put((byte)0xff);
    buffer.put((byte)0xff);
    buffer.put("/status/json".getBytes(Charset.defaultCharset()));
    byte[] checkStatusKey = buffer.array();

    FDB fdb = FDB.selectAPIVersion(610);
    try(Database db = fdb.open(clusterFile.toString())) {

      // Get Status special key from the database
      String status = db.run(tr -> {
        byte[] result = tr.get(checkStatusKey).join();
        return new String(result);
      });
      JSONObject obj = new JSONObject(status);
      System.out.println("Fdb Health   : " + obj.getJSONObject("client").getJSONObject("database_status").getBoolean("healthy"));
      assertTrue(obj.getJSONObject("client").getJSONObject("database_status").getBoolean("healthy"));
    }
  }

  @RepeatedTest(value = 3)
  public void testCRUDSchema1(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(RecordStoreProtocolTest.Person.getDescriptor());


    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .setName("Person")
      .addPrimaryKeyFields("id")
      .setSchema(dependencies)
      .build();

    schemaServiceVertxStub.upsert(request, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema2(Vertx vertx, VertxTestContext testContext) throws Exception {
    schemaServiceVertxStub.get(RecordStoreProtocol.GetSchemaRequest.newBuilder()
      .setTable("Person")
      .build(), response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema3(Vertx vertx, VertxTestContext testContext) throws Exception {
    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(RecordStoreProtocolTest.Person.getDescriptor());


    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .setName("Person")
      .addPrimaryKeyFields("id")
      .addIndexDefinitions(RecordStoreProtocol.IndexDefinition.newBuilder()
        .setField("name")
        .setIndexType(RecordStoreProtocol.IndexType.VALUE)
        .build())
      .setSchema(dependencies)
      .build();

    schemaServiceVertxStub.upsert(request, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema4(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(RecordStoreProtocolTest.Person.getDescriptor());

    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .setName("Person")
      .addPrimaryKeyFields("id")
      // let's forget an index, this is working as we cannot delete an Index for now
      .setSchema(dependencies)
      .build();

    schemaServiceVertxStub.upsert(request, response -> {
      if (response.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(new Throwable("should have failed"));
      }
    });
  }

  @RepeatedTest(value = 3)
  public void testCRUDSchema5(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(RecordStoreProtocolTest.Person.getDescriptor());

    // upsert old schema should be harmless
    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .setName("Person")
      .addPrimaryKeyFields("id")
      .addIndexDefinitions(RecordStoreProtocol.IndexDefinition.newBuilder()
        .setField("name").build())
      .setSchema(dependencies)
      .build();

    schemaServiceVertxStub.upsert(request, response -> {
      if (response.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema6(Vertx vertx, VertxTestContext testContext) throws Exception {
    adminServiceVertxStub.list(RecordStoreProtocol.ListContainerRequest.newBuilder().build(), response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        assertEquals(1, response.result().getContainersList().size());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema7(Vertx vertx, VertxTestContext testContext) throws Exception {
    adminServiceVertxStub.delete(RecordStoreProtocol.DeleteContainerRequest.newBuilder()
      .addContainers(DEFAULT_CONTAINER)
      .build(), response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }
}
