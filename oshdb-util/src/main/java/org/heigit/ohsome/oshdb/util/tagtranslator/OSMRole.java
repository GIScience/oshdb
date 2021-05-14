package org.heigit.ohsome.oshdb.util.tagtranslator;

/**
 * Represents an OSM role (which can be an arbitrary string).
 */
public class OSMRole {
  private String role;

  public OSMRole(String role) {
    this.role = role;
  }

  @Override
  public String toString() {
    return this.role;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSMRole && ((OSMRole) o).role.equals(this.role);
  }

  @Override
  public int hashCode() {
    return this.role.hashCode();
  }
}
