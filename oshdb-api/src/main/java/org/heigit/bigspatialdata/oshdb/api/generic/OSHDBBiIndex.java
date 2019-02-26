package org.heigit.bigspatialdata.oshdb.api.generic;

import java.io.Serializable;
import java.util.Objects;

class OSHDBBiIndex<U, V> implements Serializable {
  protected U index1;
  protected V index2;

  public OSHDBBiIndex(U index1, V index2) {
    this.index1 = index1;
    this.index2 = index2;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof OSHDBBiIndex)) {
      return false;
    }
    OSHDBBiIndex other = (OSHDBBiIndex) obj;
    return Objects.equals(index1, other.index1) && Objects.equals(index2, other.index2);
  }

  
  
  @Override
  public int hashCode() {
    return Objects.hash(index1, index2);
  }
}
