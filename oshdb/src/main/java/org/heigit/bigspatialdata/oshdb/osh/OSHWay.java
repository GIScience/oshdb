package org.heigit.bigspatialdata.oshdb.osh;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;

public interface OSHWay extends OSHEntity {
  
  @Override
  default OSMType getType() {
    return OSMType.WAY;
  }

  @Override
  Iterable<OSMWay> getVersions();
}
