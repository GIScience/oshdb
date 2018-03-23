package org.heigit.bigspatialdata.oshdb.api.generic;

import org.jetbrains.annotations.NotNull;

public class OSHDBCombinedIndex<U, V> extends OSHDBBiIndex<U, V> implements Comparable<OSHDBCombinedIndex<U, V>> {
  public OSHDBCombinedIndex(U index1, V index2) {
    super(index1, index2);
  }

  public U getFirstIndex() {
    return this._payload.getLeft();
  }

  public V getSecondIndex() {
    return this._payload.getRight();
  }

  @Override
  public int compareTo(@NotNull OSHDBCombinedIndex o) {
    return this._payload.compareTo(o._payload);
  }

  @Override
  public String toString() {
    return this.getFirstIndex().toString() + "&" + this.getSecondIndex().toString();
  }
}
