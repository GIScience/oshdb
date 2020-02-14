package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

class TypeFilter implements Filter {
  final OSMType type;

  TypeFilter(OSMType type) {
    this.type = type;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return e.getType() == type;
  }

  @Override
  public String toString() {
    return "type:" + type.toString();
  }
}
