/**
 * Copyright 2020 Pierre Zemb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.pierrezemb.recordstore.graphql;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.google.protobuf.Descriptors;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.utils.graphql.ProtoToGql;
import fr.pierrezemb.recordstore.utils.graphql.SchemaOptions;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLScalarType;
import graphql.schema.idl.SchemaPrinter;

import java.util.stream.Collectors;

public class GraphQLSchemaGenerator {
  public static String generate(RecordMetaData recordMetaData) {

    SchemaPrinter schemaPrinter = new SchemaPrinter();

    // Converting struct from protobuf to graphQL
    String schema = recordMetaData.getRecordTypes().values().stream()
      .map(e -> ProtoToGql.convert(e.getDescriptor(), SchemaOptions.defaultOptions()))
      .map(schemaPrinter::print)
      .collect(Collectors.joining("\n"));

    schema += "\ntype Query {\n";

    // for each type, we can retrieve all the associated queries
    schema += recordMetaData.getRecordTypes().values().stream()
      .map(GraphQLSchemaGenerator::generateQueries)
      .collect(Collectors.joining("\n"));

    schema += "}\n";

    return schema;
  }

  private static String generateQueries(RecordType e) {
    StringBuilder stringBuilder = new StringBuilder();

    stringBuilder.append("  # retrieve a list of ").append(e.getName()).append(". Use limit to retrieve only a number of records\n");
    stringBuilder.append("  all").append(e.getName()).append("s(limit: Int): [").append(e.getName()).append("!]!\n\n");


    for (Index index : e.getAllIndexes()) {
      generateQueryWithIndex(index, e, stringBuilder);
    }

    return stringBuilder.toString();
  }

  private static void generateQueryWithIndex(Index i, RecordType recordType, StringBuilder stringBuilder) {

    if (i.getName().endsWith(RecordStoreProtocol.IndexType.VALUE.toString())) {
      String[] splits = i.getName().split("_");

      if (splits.length != 4) {
        return;
      }

      String type = splits[0];
      String field = splits[2];

      Descriptors.FieldDescriptor protoField = recordType.getDescriptor().findFieldByName(field);
      if (protoField == null) {
        return;
      }

      GraphQLFieldDefinition graphqlField = ProtoToGql.convertField(protoField, SchemaOptions.defaultOptions());
      if (graphqlField == null) {
        return;
      }

      String graphQLType = "unknown";

      if (graphqlField.getType() instanceof GraphQLScalarType) {
        graphQLType = ((GraphQLScalarType) graphqlField.getType()).getName();
      }

      String fieldNameWithUpper = field.substring(0, 1).toUpperCase() + field.substring(1);

      stringBuilder
        .append("  # get a ").append(type).append(" using the ").append(field).append(" field\n")
        .append("  get").append(type).append("By").append(fieldNameWithUpper).append("(").append(field).append(": ").append(graphQLType).append(")").append(": ").append(type).append("!\n\n");
    }
  }
}
