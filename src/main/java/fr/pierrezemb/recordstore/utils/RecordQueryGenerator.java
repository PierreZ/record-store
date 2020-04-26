package fr.pierrezemb.recordstore.utils;

import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import java.util.List;

public class RecordQueryGenerator {
  public static RecordQuery generate(RecordStoreProtocol.QueryRequest request) {
    RecordQuery.Builder queryBuilder = RecordQuery.newBuilder()
      .setRecordType(request.getTable());

    if (request.getQueryFiltersList().size() == 1) {
      RecordStoreProtocol.QueryFilter queryFilter = request.getQueryFiltersList().get(0);

      switch (queryFilter.getOperation()) {

        case GREATER_THAN_OR_EQUALS:
          break;
        case LESS_THAN_OR_EQUALS:
          break;

        case EQUALS:
          switch (queryFilter.getValueCase()) {
            case STRING_VALUE:
              queryBuilder.setFilter(Query.field(queryFilter.getField()).equalsValue(queryFilter.getStringValue()));
              break;
            case INT64_VALUE:
              queryBuilder.setFilter(Query.field(queryFilter.getField()).equalsValue(queryFilter.getInt64Value()));
              break;
            case VALUE_NOT_SET:
              break;
          }
          break;

        case UNRECOGNIZED:
          break;
      }
    }

    return queryBuilder.build();
  }
}
