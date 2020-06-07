package fr.pierrezemb.recordstore.fdb;

import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;

public class UniversalIndexes {
  public static final String COUNT_INDEX_NAME = "globalRecordCount";
  public static final Index COUNT_INDEX = new Index(COUNT_INDEX_NAME,
    new GroupingKeyExpression(RecordTypeKeyExpression.RECORD_TYPE_KEY, 0), IndexTypes.COUNT);

  public static final String COUNT_UPDATES_INDEX_NAME = "globalRecordUpdateCount";
  public static final Index COUNT_UPDATES_INDEX = new Index(COUNT_UPDATES_INDEX_NAME,
    new GroupingKeyExpression(RecordTypeKeyExpression.RECORD_TYPE_KEY, 0), IndexTypes.COUNT_UPDATES);

  public static IndexAggregateFunction INDEX_COUNT_AGGREGATE_FUNCTION = new IndexAggregateFunction(
    FunctionNames.COUNT, COUNT_INDEX.getRootExpression(), COUNT_INDEX.getName());
  public static IndexAggregateFunction INDEX_COUNT_UPDATES_AGGREGATE_FUNCTION = new IndexAggregateFunction(
    FunctionNames.COUNT_UPDATES, COUNT_UPDATES_INDEX.getRootExpression(), COUNT_UPDATES_INDEX.getName());
}
