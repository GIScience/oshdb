package org.heigit.bigspatialdata.oshdb.osm2.impl;

import org.heigit.bigspatialdata.oshdb.osh2.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm2.OSMMember;

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
