package org.heigit.ohsome.oshdb.store;

import static org.heigit.ohsome.oshdb.osm.OSMType.NODE;
import static org.heigit.ohsome.oshdb.osm.OSMType.RELATION;
import static org.heigit.ohsome.oshdb.osm.OSMType.WAY;

import org.heigit.ohsome.oshdb.osm.OSMType;

public enum BackRefType {
  NODE_WAY(NODE, WAY),
  NODE_RELATION(NODE, RELATION),
  WAY_RELATION(WAY, RELATION),
  RELATION_RELATION(RELATION, RELATION);

  private final OSMType type;
  private final OSMType backRef;

  BackRefType(OSMType type, OSMType backRef) {
    this.type = type;
    this.backRef = backRef;
  }

  public OSMType getType() {
    return type;
  }

  public OSMType getBackRef() {
    return backRef;
  }

  @Override
  public String toString() {
    return name().toLowerCase();
  }
}
