package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

class TypeFilter extends Filter {
  final String type;

  TypeFilter(String type) {
    this.type = type;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return e.getType().toString().equalsIgnoreCase(type);
  }

  @Override
  public String toString() {
    return "type:" + type;
  }
}
