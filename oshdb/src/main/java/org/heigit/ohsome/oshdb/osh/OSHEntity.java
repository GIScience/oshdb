package org.heigit.ohsome.oshdb.osh;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;

public interface OSHEntity extends OSHDBBoundable {

  long getId();

  OSMType getType();

  @Deprecated(since = "0.7.0", forRemoval = true)
  int[] getRawTagKeys();

  Iterable<OSHDBTagKey> getTagKeys();

  boolean hasTagKey(OSHDBTagKey tag);

  boolean hasTagKey(int key);

  Iterable<? extends OSMEntity> getVersions();

  default List<OSHNode> getNodes() throws IOException {
    return Collections.emptyList();
  }

  default List<OSHWay> getWays() throws IOException {
    return Collections.emptyList();
  }
}
