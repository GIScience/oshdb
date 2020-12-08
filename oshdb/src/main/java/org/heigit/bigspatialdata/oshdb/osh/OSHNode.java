package org.heigit.bigspatialdata.oshdb.osh;

import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public interface OSHNode extends OSHEntity {
  
  @Override
  default OSMType getType() {
    return OSMType.NODE;
  }

  @Override
  Iterable<OSMNode> getVersions();
}
