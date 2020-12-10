package org.heigit.ohsome.oshdb.osh;

import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMType;

public interface OSHNode extends OSHEntity {
  
  @Override
  default OSMType getType() {
    return OSMType.NODE;
  }

  @Override
  Iterable<OSMNode> getVersions();
}
