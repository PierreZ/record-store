package fr.pierrezemb.recordstore.query;

import com.apple.foundationdb.record.query.RecordQuery;
import graphql.schema.DataFetchingEnvironment;

public class GraphQLQueryGenerator {
  public static RecordQuery generate(DataFetchingEnvironment env) {
    RecordQuery.Builder queryBuilder = RecordQuery.newBuilder()
      .setRecordType("Person");
    return queryBuilder.build();
  }
}
