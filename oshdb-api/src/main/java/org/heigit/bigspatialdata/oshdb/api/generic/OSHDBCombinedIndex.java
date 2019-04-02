package org.heigit.bigspatialdata.oshdb.api.generic;

import java.io.Serializable;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.jetbrains.annotations.NotNull;

public class OSHDBCombinedIndex<
        U extends Comparable<U> & Serializable,
        V extends Comparable<V> & Serializable>
    implements Comparable<OSHDBCombinedIndex<U, V>>, Serializable {

  private U index1;
  private V index2;

  public OSHDBCombinedIndex(U index1, V index2) {
    this.index1 = index1;
    this.index2 = index2;
  }

  public U getFirstIndex() {
    return this.index1;
  }

  public V getSecondIndex() {
    return this.index2;
  }

  @Override
  public int compareTo(@NotNull OSHDBCombinedIndex<U,V> other) {
    int c = this.index1.compareTo(other.index1);
    if (c == 0) {
      c = this.index2.compareTo(other.index2);
    }
    return c;
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || obj instanceof OSHDBCombinedIndex
        && java.util.Objects.equals(this.index1, ((OSHDBCombinedIndex) obj).index1)
        && java.util.Objects.equals(this.index2, ((OSHDBCombinedIndex) obj).index2);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(index1, index2);
  }
 
  @Override
  public String toString() {
    return this.getFirstIndex().toString() + "&" + this.getSecondIndex().toString();
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
  public static <A, U extends Comparable<U> & Serializable, V extends Comparable<V> & Serializable>
      SortedMap<U, SortedMap<V, A>> nest(
      Map<OSHDBCombinedIndex<U, V>, A> result
  ) {
    TreeMap<U, SortedMap<V, A>> ret = new TreeMap<>();
    result.forEach((index, data) -> {
      if (!ret.containsKey(index.getFirstIndex())) {
        ret.put(index.getFirstIndex(), new TreeMap<V, A>());
      }
      ret.get(index.getFirstIndex()).put(index.getSecondIndex(), data);
    });
    return ret;
  }
}
