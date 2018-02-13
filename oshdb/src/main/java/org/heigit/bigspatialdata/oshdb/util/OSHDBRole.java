package org.heigit.bigspatialdata.oshdb.util;

public class OSHDBRole {
  private int role;

  public OSHDBRole(int role) {
    this.role = role;
  }

  public int toInt() {
    return this.role;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSHDBRole && ((OSHDBRole)o).role == this.role;
  }

  @Override
  public int hashCode() {
    return this.role;
  }

  @Override
  public String toString() {
    return Integer.toString(this.role);
  }
}
