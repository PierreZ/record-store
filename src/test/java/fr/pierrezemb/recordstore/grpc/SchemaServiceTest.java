package fr.pierrezemb.recordstore.grpc;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocolTest;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import fr.pierrezemb.recordstore.utils.ProtobufReflectionUtil;
import io.grpc.ManagedChannel;
import io.vertx.core.json.JsonObject;
import fr.pierrezemb.recordstore.FoundationDBContainer;
import fr.pierrezemb.recordstore.MainVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.grpc.VertxChannelBuilder;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Taken from https://github.com/etcd-io/jetcd/blob/jetcd-0.5.0/jetcd-core/src/test/java/io/etcd/jetcd/KVTest.java
 */
@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SchemaServiceTest {

  private final FoundationDBContainer container = new FoundationDBContainer();
  private File clusterFile;
  SchemaServiceGrpc.SchemaServiceVertxStub stub;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    container.start();
    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("fdb-cluster-file", clusterFile.getAbsolutePath())
      );

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
    ManagedChannel channel = VertxChannelBuilder
      .forAddress(vertx, "localhost", 8080)
      .usePlaintext(true)
      .build();

    stub = SchemaServiceGrpc.newVertxStub(channel);
  }

  @Test
  public void testCreate(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(RecordStoreProtocolTest.Person.getDescriptor());

    RecordStoreProtocol.SelfDescribedMessage selfDescribedMessage = RecordStoreProtocol.SelfDescribedMessage
      .newBuilder()
      .setDescriptorSet(dependencies)
      .build();


    RecordStoreProtocol.CreateSchemaRequest request = RecordStoreProtocol.CreateSchemaRequest
      .newBuilder()
      .setName("Person")
      .setVersion(1)
      .setSchema(selfDescribedMessage)
      .build();

    stub.create(request, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result().getResult());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }
}
