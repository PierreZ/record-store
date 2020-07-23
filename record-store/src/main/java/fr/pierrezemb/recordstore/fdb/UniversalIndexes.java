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
