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
package fr.pierrezemb.recordstore.query;

import com.apple.foundationdb.record.query.RecordQuery;
import graphql.schema.DataFetchingEnvironment;

public class GraphQLQueryGenerator {
  public static RecordQuery generate(DataFetchingEnvironment env) {
    RecordQuery.Builder queryBuilder = RecordQuery.newBuilder().setRecordType("User");
    return queryBuilder.build();
  }
}
