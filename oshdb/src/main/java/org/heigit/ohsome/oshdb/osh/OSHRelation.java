package org.heigit.ohsome.oshdb.osh;

import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;

public interface OSHRelation extends OSHEntity {
  
  @Override
  default OSMType getType() {
    return OSMType.RELATION;
  }
  
  @Override
  Iterable<OSMRelation> getVersions();
}
