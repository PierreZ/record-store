package fr.pierrezemb.recordstore.grpc;

import com.google.protobuf.DescriptorProtos;
import fr.pierrezemb.recordstore.AbstractFDBContainer;
import fr.pierrezemb.recordstore.FoundationDBContainer;
import fr.pierrezemb.recordstore.GrpcVerticle;
import fr.pierrezemb.recordstore.PortManager;
import fr.pierrezemb.recordstore.auth.BiscuitClientCredential;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.proto.DemoPersonProto;
import fr.pierrezemb.recordstore.proto.AdminServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import fr.pierrezemb.recordstore.utils.protobuf.ProtobufReflectionUtil;
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

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static fr.pierrezemb.recordstore.GrpcVerticleTest.DEFAULT_CONTAINER;
import static fr.pierrezemb.recordstore.GrpcVerticleTest.DEFAULT_TENANT;
import static org.junit.Assert.assertEquals;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SchemaAdminServiceTest extends AbstractFDBContainer {

  public final int port = PortManager.nextFreePort();
  private SchemaServiceGrpc.SchemaServiceVertxStub schemaServiceVertxStub;
  private AdminServiceGrpc.AdminServiceVertxStub adminServiceVertxStub;
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put("fdb-cluster-file", clusterFile.getAbsolutePath())
        .put("grpc-listen-port", port));

    BiscuitManager biscuitManager = new BiscuitManager();
    String sealedBiscuit = biscuitManager.create(DEFAULT_TENANT, Collections.emptyList());
    BiscuitClientCredential credentials = new BiscuitClientCredential(DEFAULT_TENANT, sealedBiscuit, this.getClass().getName());

    // deploy verticle
    vertx.deployVerticle(new GrpcVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
    ManagedChannel channel = VertxChannelBuilder
      .forAddress(vertx, "localhost", port)
      .usePlaintext(true)
      .build();

    schemaServiceVertxStub = SchemaServiceGrpc.newVertxStub(channel).withCallCredentials(credentials);
    adminServiceVertxStub = AdminServiceGrpc.newVertxStub(channel).withCallCredentials(credentials);
  }

  @RepeatedTest(value = 3)
  public void testCRUDSchema1(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoPersonProto.Person.getDescriptor());


    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .addIndexRequest(RecordStoreProtocol.IndexSchemaRequest.newBuilder()
        .setName("Person")
        .addPrimaryKeyFields("id")
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
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoPersonProto.Person.getDescriptor());


    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .addIndexRequest(RecordStoreProtocol.IndexSchemaRequest.newBuilder()
        .setName("Person")
        .addPrimaryKeyFields("id")
        .addIndexDefinitions(RecordStoreProtocol.IndexDefinition.newBuilder()
          .setField("name")
          .setIndexType(RecordStoreProtocol.IndexType.VALUE)
          .build())
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
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoPersonProto.Person.getDescriptor());

    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .addIndexRequest(RecordStoreProtocol.IndexSchemaRequest.newBuilder()
        .setName("Person")
        .addPrimaryKeyFields("id")
        // let's forget an index, this is working as we cannot delete an Index for now
        .build())
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
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoPersonProto.Person.getDescriptor());

    // upsert old schema should be harmless
    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .addIndexRequest(RecordStoreProtocol.IndexSchemaRequest.newBuilder()
        .setName("Person")
        .addPrimaryKeyFields("id")
        .addIndexDefinitions(RecordStoreProtocol.IndexDefinition.newBuilder()
          .setField("name").build())
        .build())
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
