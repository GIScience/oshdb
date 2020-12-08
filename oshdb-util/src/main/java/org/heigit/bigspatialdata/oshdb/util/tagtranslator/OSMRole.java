package org.heigit.bigspatialdata.oshdb.util.tagtranslator;

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
