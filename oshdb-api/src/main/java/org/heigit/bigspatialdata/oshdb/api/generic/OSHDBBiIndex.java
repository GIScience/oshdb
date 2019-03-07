package org.heigit.bigspatialdata.oshdb.api.generic;

import java.io.Serializable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

class OSHDBBiIndex<U, V> implements Serializable {
  Pair<U, V> _payload;

  public OSHDBBiIndex(U index1, V index2) {
    this._payload = new ImmutablePair<>(index1, index2);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof OSHDBBiIndex &&
        this._payload.equals(((OSHDBBiIndex) other)._payload);
  }
}
