package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import org.heigit.ohsome.oshdb.osm.OSMType;

public interface OSMMember {
  public long getId();

  public OSMType getType();

  public int getRoleId();

  public OSHEntity getEntity();

  public default String asString() {
    return String.format("%s:%d (%d)", getType(), getId(), getRoleId());
  }
}