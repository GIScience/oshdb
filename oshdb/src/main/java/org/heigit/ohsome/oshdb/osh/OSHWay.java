package org.heigit.ohsome.oshdb.osh;

import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;

public interface OSHWay extends OSHEntity {
  
  @Override
  default OSMType getType() {
    return OSMType.WAY;
  }

  @Override
  Iterable<OSMWay> getVersions();
}
