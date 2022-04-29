package org.heigit.ohsome.oshdb.osh;

import java.util.Collections;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;

public interface OSHEntity {

  long getId();

  OSMType getType();

  OSHDBBoundable getBounce();

  Iterable<OSHDBTagKey> getTagKeys();

  boolean hasTagKey(OSHDBTagKey tag);

  boolean hasTagKey(int key);

  Iterable<? extends OSMEntity> getVersions();

  default List<OSHNode> getNodes() {
    return Collections.emptyList();
  }

  default List<OSHWay> getWays() {
    return Collections.emptyList();
  }
}
