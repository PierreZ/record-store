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
              .addSubdirectory(
                  new KeySpaceDirectory("tenant", KeySpaceDirectory.KeyType.STRING)
                      .addSubdirectory(
                          new KeySpaceDirectory("recordSpace", KeySpaceDirectory.KeyType.STRING)
                              .addSubdirectory(
                                  new KeySpaceDirectory(
                                      "metadata", KeySpaceDirectory.KeyType.STRING, "m"))
                              .addSubdirectory(
                                  new KeySpaceDirectory(
                                      "data", KeySpaceDirectory.KeyType.STRING, "d")))));

  public static KeySpacePath openMetaDataKeySpacePath(String tenant, String recordSpace) {
    return openKeySpacePath(tenant, recordSpace, "metadata");
  }

  public static KeySpacePath openKeySpacePath(String tenant) {
    return RS_KEY_SPACE.path("application", APPLICATION_NAME).add("tenant", tenant);
  }

  public static KeySpacePath openDataKeySpacePath(String tenant, String recordSpace) {
    return openKeySpacePath(tenant, recordSpace, "data");
  }

  private static KeySpacePath openKeySpacePath(String tenant, String env, String subDirectory) {
    return RS_KEY_SPACE
        .path("application", APPLICATION_NAME)
        .add("tenant", tenant)
        .add("recordSpace", env)
        .add(subDirectory);
  }
}
