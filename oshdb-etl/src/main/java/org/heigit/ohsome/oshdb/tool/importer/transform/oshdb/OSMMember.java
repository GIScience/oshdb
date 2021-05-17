package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import org.heigit.ohsome.oshdb.osm.OSMType;

public interface OSMMember {
  long getId();

  OSMType getType();

  int getRoleId();

  OSHEntity getEntity();

  default String asString() {
    return String.format("%s:%d (%d)", getType(), getId(), getRoleId());
  }
}