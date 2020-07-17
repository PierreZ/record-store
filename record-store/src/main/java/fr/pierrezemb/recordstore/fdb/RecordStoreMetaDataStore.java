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

import com.apple.foundationdb.record.provider.foundationdb.FDBMetaDataStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

public class RecordStoreMetaDataStore {
  public static FDBMetaDataStore createMetadataStore(FDBRecordContext context, String tenant, String env) {
    FDBMetaDataStore metaDataStore = new FDBMetaDataStore(context, RecordStoreKeySpace.getMetaDataKeySpacePath(tenant, env));
    metaDataStore.setMaintainHistory(true);
    return metaDataStore;
  }
}
