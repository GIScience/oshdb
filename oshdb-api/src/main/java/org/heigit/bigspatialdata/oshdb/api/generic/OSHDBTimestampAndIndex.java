package org.heigit.bigspatialdata.oshdb.api.generic;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

public class OSHDBTimestampAndIndex<V> implements Comparable<OSHDBTimestampAndIndex>, Serializable {
  private Pair<V, OSHDBTimestamp> _payload;

  public OSHDBTimestampAndIndex(OSHDBTimestamp timeIndex, V otherIndex) {
    this._payload = new ImmutablePair<>(otherIndex, timeIndex);
  }

  public OSHDBTimestamp getTimeIndex() {
    return this._payload.getRight();
  }

  public V getOtherIndex() {
    return this._payload.getLeft();
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
