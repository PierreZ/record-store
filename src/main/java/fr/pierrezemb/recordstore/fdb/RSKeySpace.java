package fr.pierrezemb.recordstore.fdb;

import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;

public class RSKeySpace {
  public static final String APPLICATION_NAME = "record-store";
  public static final KeySpace RS_KEY_SPACE =
    new KeySpace(
      new DirectoryLayerDirectory("application")
        .addSubdirectory(new KeySpaceDirectory("tenant", KeySpaceDirectory.KeyType.STRING)
          .addSubdirectory(new KeySpaceDirectory("metadata", KeySpaceDirectory.KeyType.STRING, "m"))
          .addSubdirectory(new KeySpaceDirectory("data", KeySpaceDirectory.KeyType.STRING, "d"))
        ));

  public static KeySpacePath getMetaDataKeySpacePath(String tenant) {
    return getKeySpacePath(tenant, "metadata");
  }

  public static KeySpacePath getDataKeySpacePath(String tenant) {
    return getKeySpacePath(tenant, "data");
  }

  private static KeySpacePath getKeySpacePath(String tenant, String subDirectory) {
    return RS_KEY_SPACE
      .path("application", APPLICATION_NAME)
      .add("tenant", tenant)
      .add(subDirectory);
  }
}
