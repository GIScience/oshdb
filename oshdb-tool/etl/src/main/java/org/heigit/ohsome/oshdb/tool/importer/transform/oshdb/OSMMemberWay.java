package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import org.heigit.ohsome.oshdb.osm.OSMType;

public class OSMMemberWay implements OSMMember {

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
