package org.heigit.bigspatialdata.oshdb.osh;

import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public interface OSHRelation extends OSHEntity {
  
  @Override
  default OSMType getType() {
    return OSMType.RELATION;
  }
  
  @Override
  Iterable<OSMRelation> getVersions();
}
