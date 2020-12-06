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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;

public class RecordStoreKeySpace {
  public static final String APPLICATION_NAME = "record-store";
  public static final KeySpace RS_KEY_SPACE =
    new KeySpace(
      new DirectoryLayerDirectory("application")
        .addSubdirectory(new KeySpaceDirectory("tenant", KeySpaceDirectory.KeyType.STRING)
          .addSubdirectory(new KeySpaceDirectory("recordSpaceType", KeySpaceDirectory.KeyType.STRING)
            .addSubdirectory(new KeySpaceDirectory("recordSpace", KeySpaceDirectory.KeyType.STRING)
              .addSubdirectory(new KeySpaceDirectory("metadata", KeySpaceDirectory.KeyType.STRING, "m"))
              .addSubdirectory(new KeySpaceDirectory("data", KeySpaceDirectory.KeyType.STRING, "d"))
            ))));

  public static KeySpacePath getMetaDataKeySpacePath(String tenant, String recordSpace) {
    return getKeySpacePath(tenant, recordSpace, "unmanaged", "metadata");
  }

  public static KeySpacePath getManagedKeySpacePath(String tenant) {
    return RS_KEY_SPACE
      .path("application", APPLICATION_NAME)
      .add("tenant", tenant).add("recordSpaceType", "managed");
  }

  public static KeySpacePath getManagedDataKeySpacePath(String tenant, String recordSpace) {
    return getKeySpacePath(tenant, recordSpace, "managed", "data");
  }

  public static KeySpacePath getUnManagedDataKeySpacePath(String tenant, String recordSpace) {
    return getKeySpacePath(tenant, recordSpace, "unmanaged", "data");
  }

  private static KeySpacePath getKeySpacePath(String tenant, String env, String recordSpaceType, String subDirectory) {
    return RS_KEY_SPACE
      .path("application", APPLICATION_NAME)
      .add("tenant", tenant)
      .add("recordSpaceType", recordSpaceType)
      .add("recordSpace", env)
      .add(subDirectory);
  }
}
