package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;

public class TypeFilter implements FilterExpression {
  private final OSMType type;

  TypeFilter(OSMType type) {
    this.type = type;
  }

  /**
   * Returns the OSM type of this filter.
   *
   * @return the OSM type of this filter.
   */
  public OSMType getType() {
    return this.type;
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
