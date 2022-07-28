package org.heigit.ohsome.oshdb.api.mapreducer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CombinedIndex<U, V> implements Serializable {
  private final U u;
  private final V v;

  public CombinedIndex(U u, V v) {
    this.u = u;
    this.v = v;
  }

  public U u() {
    return u;
  }

  public V v() {
    return v;
  }

  @Override
  public int hashCode() {
    return Objects.hash(u, v);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof CombinedIndex))
      return false;
    CombinedIndex other = (CombinedIndex) obj;
    return Objects.equals(u, other.u) && Objects.equals(v, other.v);
  }

  /**
   * Helper function that converts the dual-index data structure returned by aggregation operations
   * on this object to a nested Map structure, which can be easier to process further on.
   *
   * <p>This version creates a map for each &lt;U&gt; index value, containing maps containing
   * results by timestamps.</p>
   *
   * @param result the "flat" result data structure that should be converted to a nested structure
   * @param <A> an arbitrary data type, used for the data value items
   * @param <U> an arbitrary data type, used for the index'es key items
   * @param <V> an arbitrary data type, used for the index'es key items
   * @return a nested data structure: for each index part there is a separate level of nested maps
   */
  public static <A, U, V> Map<U, Map<V, A>> nest(Map<CombinedIndex<U, V>, A> result) {
    Map<U, Map<V, A>> ret = new HashMap<>();
    result.forEach((index, data) -> ret.computeIfAbsent(index.u(), x -> new HashMap<>())
        .put(index.v(), data));
    return ret;
  }
}
