package org.heigit.bigspatialdata.oshdb.util;

import java.io.Serializable;

public class OSHDBTagKey implements Serializable {
  private static final long serialVersionUID = 1L;
  private int key;

  public OSHDBTagKey(int key) {
    this.key = key;
  }

  public int toInt() {
    return this.key;
  }

  public boolean isPresentInKeytables() {
    return this.key >= 0;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSHDBTagKey && ((OSHDBTagKey)o).key == this.key;
  }

  @Override
  public int hashCode() {
    return this.key;
  }

  @Override
  public String toString() {
    return Integer.toString(this.key);
  }
}
