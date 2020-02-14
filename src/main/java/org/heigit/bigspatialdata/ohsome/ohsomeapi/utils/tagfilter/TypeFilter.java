package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import com.google.common.collect.Streams;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

class TypeFilter implements FilterExpression {
  final OSMType type;

  TypeFilter(OSMType type) {
    this.type = type;
  }

  @Override
  public boolean applyOSM(OSMEntity e) {
    return e.getType() == type;
  }

  @Override
  public boolean applyOSH(OSHEntity e) {
    return e.getType() == type;
  }

  @Override
  public String toString() {
    return "type:" + type.toString();
  }
}
