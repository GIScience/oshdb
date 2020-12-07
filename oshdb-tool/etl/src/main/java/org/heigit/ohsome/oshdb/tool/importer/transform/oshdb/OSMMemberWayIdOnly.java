package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import org.heigit.ohsome.oshdb.osm.OSMType;

public class OSMMemberWayIdOnly implements OSMMember{

  private final long id;
    
  public OSMMemberWayIdOnly(long id) {
    this.id = id;
  }

  @Override
  public long getId() {
    return id;
  }

  @Override
  public OSMType getType() {
    return OSMType.NODE;
  }

  @Override
  public int getRoleId() {
    return -1;
  }

  @Override
  public OSHEntity getEntity() {
    return null;
  }

  
  @Override
  public String toString() {
    return asString();
  }
}
