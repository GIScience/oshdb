package org.heigit.bigspatialdata.oshdb.osh;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;

public interface OSHEntity {

  long getId();

  OSMType getType();

  OSHDBBoundingBox getBoundingBox();

  int[] getRawTagKeys();

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
