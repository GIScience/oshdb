package org.heigit.bigspatialdata.oshdb.api.generic;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.jetbrains.annotations.NotNull;

public class OSHDBTimestampAndIndex<V> extends OSHDBBiIndex<OSHDBTimestamp, V> implements
    Comparable<OSHDBTimestampAndIndex<V>> {

  public OSHDBTimestampAndIndex(OSHDBTimestamp timeIndex, V otherIndex) {
    super(timeIndex, otherIndex);
  }

  public OSHDBTimestamp getTimeIndex() {
    return this._payload.getLeft();
  }

  public V getOtherIndex() {
    return this._payload.getRight();
  }

  @Override
  public int compareTo(@NotNull OSHDBTimestampAndIndex o) {
    return this._payload.compareTo(o._payload);
  }

  @Override
  public String toString() {
    return this.getOtherIndex().toString() + "@" + this.getTimeIndex().toString();
  }
}
