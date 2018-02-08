package org.heigit.bigspatialdata.oshdb.osm2.impl;

import org.heigit.bigspatialdata.oshdb.osh2.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm2.OSMMember;

public class OSMMemberWay implements OSMMember{

  private final long id;
  private final OSHNode node;
  
  
  
  public OSMMemberWay(long id, OSHNode node) {
    this.id = id;
    this.node = node;
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
  public OSHNode getEntity() {
    return node;
  }

  @Override
  public String toString() {
    return asString();
  }
}
