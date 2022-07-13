package org.heigit.ohsome.oshdb.api.mapreducer.aggregation;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;

public class Agg {
  private Agg() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static Long sumLong(MapReducerBase<?, Long> mr) {
    return mr.reduce(() -> 0L, Long::sum, Long::sum);
  }

  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static Long sumInt(MapReducerBase<?, Integer> mr) {
    return mr.reduce(() -> 0L, Long::sum, Long::sum);
  }

  /**
   * Gets all unique values of the results.
   *
   * <p>
   * For example, this can be used together with the OSMContributionView to get the total amount of
   * unique users editing specific feature types.
   * </p>
   *
   * @return the set of distinct values
   */
  public static <T> Set<T> uniq(MapReducerBase<?, T> mr) {
    return mr.reduce(Agg::uniqIdentitySupplier, Agg::uniqAccumulator, Agg::uniqCombiner);
  }

  static <T> Set<T> uniqIdentitySupplier() {
    return new HashSet<>();
  }

  static <T> Set<T> uniqAccumulator(Set<T> acc, T cur) {
    acc.add(cur);
    return acc;
  }

  static <T> Set<T> uniqCombiner(Set<T> a, Set<T> b) {
    HashSet<T> result = new HashSet<>((int) Math.ceil(Math.max(a.size(), b.size()) / 0.75));
    result.addAll(a);
    result.addAll(b);
    return result;
  }

  /**
   * Mutable version of WeightedValue type.
   *
   * <p>
   * For internal use to do faster aggregation during reduce operations.
   * </p>
   */
  private static class MutableWeightedDouble implements Serializable {
    double num;
    double weight;

    private MutableWeightedDouble(double num, double weight) {
      this.num = num;
      this.weight = weight;
    }

    double average() {
      return num / weight;
    }

    static MutableWeightedDouble identitySupplier() {
      return new MutableWeightedDouble(0.0, 0.0);
    }

    static MutableWeightedDouble accumulator(MutableWeightedDouble acc, WeightedValue cur) {
      acc.num = acc.num + cur.getValue().doubleValue() * cur.getWeight();
      acc.weight += cur.getWeight();
      return acc;
    }

    static MutableWeightedDouble combiner(MutableWeightedDouble a, MutableWeightedDouble b) {
      return new MutableWeightedDouble(a.num + b.num, a.weight + b.weight);
    }
  }
}
