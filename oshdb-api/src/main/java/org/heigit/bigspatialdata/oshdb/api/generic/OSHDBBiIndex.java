package org.heigit.bigspatialdata.oshdb.api.generic;

import java.io.Serializable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

class OSHDBBiIndex<U, V> implements Serializable {
  Pair<U, V> payload;

  public OSHDBBiIndex(U index1, V index2) {
    this.payload = new ImmutablePair<>(index1, index2);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof OSHDBBiIndex && this.payload.equals(((OSHDBBiIndex) other).payload);
  }

  @Override
  public int hashCode() {
    return this.payload.hashCode();
  }
}
