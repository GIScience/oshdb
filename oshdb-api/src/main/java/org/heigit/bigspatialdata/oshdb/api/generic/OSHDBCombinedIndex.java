package org.heigit.bigspatialdata.oshdb.api.generic;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
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

  /**
   * Helper function that converts the dual-index data structure returned by aggregation operations
   * on this object to a nested Map structure, which can be easier to process further on.
   *
   * This version creates a map for each &lt;U&gt; index value, containing maps containing results
   * by timestamps.
   *
   * @param result the "flat" result data structure that should be converted to a nested structure
   * @param <A> an arbitrary data type, used for the data value items
   * @param <U> an arbitrary data type, used for the index'es key items
   * @param <V> an arbitrary data type, used for the index'es key items
   * @return a nested data structure: for each index part there is a separate level of nested maps
   */
  public static <A, U, V> SortedMap<U, SortedMap<V, A>> nest(
      Map<OSHDBCombinedIndex<U, V>, A> result
  ) {
    TreeMap<U, SortedMap<V, A>> ret = new TreeMap<>();
    result.forEach((index, data) -> {
      if (!ret.containsKey(index.getFirstIndex()))
        ret.put(index.getFirstIndex(), new TreeMap<V, A>());
      ret.get(index.getFirstIndex()).put(index.getSecondIndex(), data);
    });
    return ret;
  }
}
