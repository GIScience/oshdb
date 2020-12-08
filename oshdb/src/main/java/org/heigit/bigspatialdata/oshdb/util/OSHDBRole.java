package org.heigit.bigspatialdata.oshdb.util;

import java.io.Serializable;

public class OSHDBRole implements Serializable {
  private static final long serialVersionUID = 1L;
  private int role;

  public OSHDBRole(int role) {
    this.role = role;
  }

  public int toInt() {
    return this.role;
  }

  public boolean isPresentInKeytables() {
    return this.role >= 0;
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
