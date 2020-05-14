package fr.pierrezemb.recordstore.datasets;

import com.github.javafaker.Faker;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.datasets.proto.DemoPersonProto;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.utils.ProtobufReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class PersonDataset implements Dataset {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersonDataset.class);

  @Override
  public void load(RecordLayer recordLayer, String tenant, String container, int nbrRecord) throws Descriptors.DescriptorValidationException, InvalidProtocolBufferException {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoPersonProto.Person.getDescriptor());

    recordLayer.upsertSchema(
      tenant,
      container,
      "Person",
      dependencies,
      ImmutableList.of(
        RecordStoreProtocol.IndexDefinition.newBuilder()
          .setIndexType(RecordStoreProtocol.IndexType.VALUE)
          .setField("name").build(),
        RecordStoreProtocol.IndexDefinition.newBuilder()
          .setIndexType(RecordStoreProtocol.IndexType.VALUE)
          .setField("email")
          .build()),
      ImmutableList.of("id")
    );

    Faker faker = new Faker();
    FakeValuesService fakeValuesService = new FakeValuesService(
      new Locale("en-US"), new RandomService());

    for (int i = 0; i < nbrRecord; i++) {
      DemoPersonProto.Person person = DemoPersonProto.Person.newBuilder()
        .setId(i)
        .setName(faker.funnyName().name())
        .setEmail(fakeValuesService.bothify("???????????##@gmail.com"))
        .build();

      recordLayer.putRecord(tenant, container, "Person", person.toByteArray());
    }
  }
}
