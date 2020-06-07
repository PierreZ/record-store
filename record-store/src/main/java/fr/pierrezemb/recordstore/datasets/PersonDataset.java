package fr.pierrezemb.recordstore.datasets;

import com.github.javafaker.Faker;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.datasets.proto.DemoPersonProto;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.utils.protobuf.ProtobufReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class PersonDataset implements Dataset {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersonDataset.class);

  @Override
  public void load(RecordLayer recordLayer, String tenant, String container, int nbrRecord) throws Descriptors.DescriptorValidationException, InvalidProtocolBufferException {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoPersonProto.Person.getDescriptor());

    recordLayer.upsertSchema(
      tenant,
      container,
      dependencies,
      ImmutableList.of(
        RecordStoreProtocol.IndexSchemaRequest.newBuilder()
          .setName("Person")
          .addAllIndexDefinitions(ImmutableList.of(
            RecordStoreProtocol.IndexDefinition.newBuilder()
              .setIndexType(RecordStoreProtocol.IndexType.VALUE)
              .setFanType(RecordStoreProtocol.FanType.FAN_OUT)
              .setField("beers").build(),
            RecordStoreProtocol.IndexDefinition.newBuilder()
              .setIndexType(RecordStoreProtocol.IndexType.VALUE)
              .setField("name").build(),
            RecordStoreProtocol.IndexDefinition.newBuilder()
              .setIndexType(RecordStoreProtocol.IndexType.VALUE)
              .setField("email")
              .build(),
            RecordStoreProtocol.IndexDefinition.newBuilder()
              .setIndexType(RecordStoreProtocol.IndexType.MAP_KEYS_AND_VALUES)
              .setField("favorite_locations_from_tv")
              .build(),
            RecordStoreProtocol.IndexDefinition.newBuilder()
              .setIndexType(RecordStoreProtocol.IndexType.TEXT_DEFAULT_TOKENIZER)
              .setField("rick_and_morty_quotes")
              .build(),
            RecordStoreProtocol.IndexDefinition.newBuilder()
              .setField("address")
              .setNestedIndex(RecordStoreProtocol.IndexDefinition.newBuilder()
                .setField("city")
                .build())
              .build()))
          .addAllPrimaryKeyFields(ImmutableList.of("id"))
          .build()
      )
    );

    Faker faker = new Faker(new Random(42));

    for (int i = 0; i < nbrRecord; i++) {

      ArrayList<String> beers = new ArrayList<>();
      for (int j = 0; j < 5; j++) {
        beers.add(faker.beer().name());
      }

      HashMap<String, String> favoritePlanets = new HashMap<>();
      favoritePlanets.put("hitchhikers_guide_to_the_galaxy", faker.hitchhikersGuideToTheGalaxy().planet());
      favoritePlanets.put("rick_and_morty", faker.rickAndMorty().location());
      favoritePlanets.put("star_trek", faker.starTrek().location());

      DemoPersonProto.Address address = DemoPersonProto.Address.newBuilder()
        .setFullAddress(faker.address().fullAddress())
        .setCity(faker.address().cityName())
        .build();

      DemoPersonProto.Person person = DemoPersonProto.Person.newBuilder()
        .setId(i)
        .setName(faker.funnyName().name())
        .setEmail(faker.internet().emailAddress())
        .addAllBeers(beers)
        .setRickAndMortyQuotes(faker.rickAndMorty().quote())
        .putAllFavoriteLocationsFromTv(favoritePlanets)
        .setAddress(address)
        .build();

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("inserting Person '{}'", person);
      }

      recordLayer.putRecord(tenant, container, "Person", person.toByteArray());
    }
  }
}
