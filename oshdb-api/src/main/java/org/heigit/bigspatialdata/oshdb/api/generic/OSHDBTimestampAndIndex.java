package org.heigit.bigspatialdata.oshdb.api.generic;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
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

  /**
   * Helper function that converts the dual-index data structure returned by aggregation operations
   * on this object to a nested Map structure, which can be easier to process further on.
   *
   * This version creates a map for each &lt;U&gt; index value, containing maps containing results
   * by timestamps.
   *
   * See also {@link #nestTimeThenIndex(Map)}.
   *
   * @param result the "flat" result data structure that should be converted to a nested structure
   * @param <A> an arbitrary data type, used for the data value items
   * @param <U> an arbitrary data type, used for the index'es key items
   * @return a nested data structure: for each index part there is a separate level of nested maps
   */
  public static <A, U> SortedMap<U, SortedMap<OSHDBTimestamp, A>> nestIndexThenTime(Map<OSHDBTimestampAndIndex<U>, A> result) {
    TreeMap<U, SortedMap<OSHDBTimestamp, A>> ret = new TreeMap<>();
    result.forEach((index, data) -> {
      if (!ret.containsKey(index.getOtherIndex()))
        ret.put(index.getOtherIndex(), new TreeMap<OSHDBTimestamp, A>());
      ret.get(index.getOtherIndex()).put(index.getTimeIndex(), data);
    });
    return ret;
  }

  /**
   * Helper function that converts the dual-index data structure returned by aggregation operations
   * on this object to a nested Map structure, which can be easier to process further on.
   *
   * This version creates a map for each timestamp, containing maps containing results by &lt;U&gt;
   * index values.
   *
   * See also {@link #nestIndexThenTime(Map)}.
   *
   * @param result the "flat" result data structure that should be converted to a nested structure
   * @param <A> an arbitrary data type, used for the data value items
   * @param <U> an arbitrary data type, used for the index'es key items
   * @return a nested data structure: for each index part there is a separate level of nested maps
   */
  public static <A, U> SortedMap<OSHDBTimestamp, SortedMap<U, A>> nestTimeThenIndex(Map<OSHDBTimestampAndIndex<U>, A> result) {
    TreeMap<OSHDBTimestamp, SortedMap<U, A>> ret = new TreeMap<>();
    result.forEach((index, data) -> {
      if (!ret.containsKey(index.getTimeIndex()))
        ret.put(index.getTimeIndex(), new TreeMap<U, A>());
      ret.get(index.getTimeIndex()).put(index.getOtherIndex(), data);
    });
    return ret;
  }
}
