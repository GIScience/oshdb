package org.heigit.bigspatialdata.oshdb.osm2.impl;

import org.heigit.bigspatialdata.oshdb.osh2.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm2.OSMMember;

public class OSMMemberRelation implements OSMMember {

  private final long id;
  private final OSMType type;
  private final int roleId;
  private final OSHEntity entity;

  public OSMMemberRelation(final long id, final OSMType type, final int roleId) {
    this(id, type, roleId, null);
  }

  public OSMMemberRelation(final long id, final OSMType type, final int roleId, OSHEntity entity) {
    this.id = id;
    this.type = type;
    this.roleId = roleId;
    this.entity = entity;
  }

  @Override
  public long getId() {
    return id;
  }

  @Override
  public OSMType getType() {
    return type;
  }

  @Override
  public int getRoleId() {
    return roleId;
  }

  @Override
  public OSHEntity getEntity() {
    return entity;
  }
  
  @Override
  public String toString() {
    return asString();
  }
}
