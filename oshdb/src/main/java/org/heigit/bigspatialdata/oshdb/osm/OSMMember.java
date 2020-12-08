package org.heigit.bigspatialdata.oshdb.osm;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBRole;

/**
 * Holds an OSH-Object that belongs to the Way or Relation this Member is contained in.
 */
public class OSMMember {
  
  
  private final long id;
  private final OSMType type;
  private final int roleId;
  private final OSHEntity entity;

  public OSMMember(final long id, final OSMType type, final int roleId) {
    this(id, type, roleId, null);
  }

  public OSMMember(final long id, final OSMType type, final int roleId,
      OSHEntity entity) {
    this.id = id;
    this.type = type;
    this.roleId = roleId;
    this.entity = entity;
  }

  public long getId() {
    return id;
  }

  public OSMType getType() {
    return type;
  }

  public int getRawRoleId() {
    return roleId;
  }

  public OSHDBRole getRoleId() {
    return new OSHDBRole(roleId);
  }

  public OSHEntity getEntity() {
    return entity;
  }

  @Override
  public String toString() {
    return String.format("T:%s ID:%d R:%d", type, id, roleId);
  }

}
