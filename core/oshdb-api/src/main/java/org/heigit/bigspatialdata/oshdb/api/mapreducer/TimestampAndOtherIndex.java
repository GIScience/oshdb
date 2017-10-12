package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.jetbrains.annotations.NotNull;

public class TimestampAndOtherIndex<V> implements Comparable<TimestampAndOtherIndex> {
  private Pair<V, OSHDBTimestamp> _payload;
  public TimestampAndOtherIndex(OSHDBTimestamp timeIndex, V otherIndex) {
    this._payload = new ImmutablePair<>(otherIndex, timeIndex);
  }
  public OSHDBTimestamp getTimeIndex() {
    return this._payload.getRight();
  }
  public V getOtherIndex() {
    return this._payload.getLeft();
  }
  @Override
  public int compareTo(@NotNull TimestampAndOtherIndex o) {
    return this._payload.compareTo(o._payload);
  }
}
