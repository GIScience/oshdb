package org.heigit.bigspatialdata.oshdb.util;

public enum OSMType {
  NODE(0),
  WAY(1),
  RELATION(2);

  private final int value;

  OSMType(final int value) {
    this.value = value;
  }

  public static OSMType fromInt(final int value) throws Exception {
    switch(value) {
      case 0:
        return NODE;
      case 1:
        return WAY;
      case 2:
        return RELATION;
    }
    throw new Exception("invalid OSM type id: "+value);
  }

  public final int intValue() {
    return this.value;
  }
}
