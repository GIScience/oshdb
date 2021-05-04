package org.heigit.ohsome.oshdb.osm;

import java.util.Objects;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.OSHDBRole;

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

  /**
   * Create a new {@code OSMMember} instance.
   */
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

  @Override
  public int hashCode() {
    return Objects.hash(id, roleId, type);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OSMMember)) {
      return false;
    }
    OSMMember other = (OSMMember) obj;
    return id == other.id && roleId == other.roleId && type == other.type;
  }



}
