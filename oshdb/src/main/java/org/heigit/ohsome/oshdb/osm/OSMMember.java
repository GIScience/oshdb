package org.heigit.ohsome.oshdb.osm;

import java.io.Serializable;
import java.util.Objects;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.osh.OSHEntity;

/**
 * Holds an OSH-Object that belongs to the Way or Relation this Member is contained in.
 */
public class OSMMember implements Serializable {
  private final long id;
  private final OSMType type;
  private final OSHDBRole role;
  private final transient OSHEntity entity;

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
    this.role = OSHDBRole.of(roleId);
    this.entity = entity;
  }

  public long getId() {
    return id;
  }

  public OSMType getType() {
    return type;
  }

  public OSHDBRole getRole() {
    return role;
  }

  public OSHEntity getEntity() {
    return entity;
  }

  @Override
  public String toString() {
    return String.format("T:%s ID:%d R:%d", type, id, role.getId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id, role.getId());
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
    return type == other.type && id == other.id && role.equals(other.role);
  }

}
