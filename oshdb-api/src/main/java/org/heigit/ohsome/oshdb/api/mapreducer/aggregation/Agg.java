package org.heigit.ohsome.oshdb.api.mapreducer.aggregation;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;

public class Agg {

  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static Long sumLong(MapReducer<Long> mr) {
    return mr.reduce(() -> 0L, Long::sum, Long::sum);
  }

  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static <U extends Comparable<U> & Serializable> SortedMap<U, Long> sumLong(
      MapAggregator<U, Long> mr) {
    return mr.reduce(() -> 0L, Long::sum, Long::sum);
  }

  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static Long sumInt(MapReducer<Integer> mr) {
    return mr.reduce(() -> 0L, Long::sum, Long::sum);
  }

  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static <U extends Comparable<U> & Serializable> SortedMap<U, Long> sumInt(
      MapAggregator<U, Integer> mr) {
    return mr.reduce(() -> 0L, Long::sum, Long::sum);
  }

  /**
   * Counts the number of results.
   *
   * @return the total count
   */
  public static Long count(MapReducer<?> mr) {
    return mr.map(x -> 1L).aggregate(Agg::sumLong);
  }

  /**
   * Counts the number of results.
   *
   * @return the total count
   */
  public static <U extends Comparable<U> & Serializable> SortedMap<U, Long> count(
      MapAggregator<U, ?> mr) {
    return mr.map(x -> 1L).aggregate(Agg::sumLong);
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
  public static <T> Set<T> uniq(MapReducer<T> mr) {
    return mr.reduce(Agg::uniqIdentitySupplier, Agg::uniqAccumulator, Agg::uniqCombiner);
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
  public static <U extends Comparable<U> & Serializable, T> SortedMap<U, Set<T>> uniq(
      MapAggregator<U, T> mr) {
    return mr.reduce(Agg::uniqIdentitySupplier, Agg::uniqAccumulator, Agg::uniqCombiner);
  }

  /**
   * Counts all unique values of the results.
   *
   * <p>
   * For example, this can be used together with the OSMContributionView to get the number of unique
   * users editing specific feature types.
   * </p>
   *
   * @return the set of distinct values
   */
  public static Integer countUniq(MapReducer<?> mr) {
    return mr.aggregate(Agg::uniq).size();
  }

  /**
   * Counts all unique values of the results.
   *
   * <p>
   * For example, this can be used together with the OSMContributionView to get the number of unique
   * users editing specific feature types.
   * </p>
   *
   * @return the set of distinct values
   */
  public static <U extends Comparable<U> & Serializable, T> SortedMap<U, Integer> countUniq(
      MapAggregator<U, T> mr) {
    var result = new TreeMap<U, Integer>();
    mr.aggregate(Agg::uniq).forEach((k, v) -> result.put(k, v.size()));
    return result;
  }

  /**
   * Calculates the weighted average of the results.
   *
   * @return the weighted average of the numbers
   */
  public static double weightedAverage(MapReducer<? extends WeightedValue> mr) {
    return mr.reduce(
        MutableWeightedDouble::identitySupplier,
        MutableWeightedDouble::accumulator,
        MutableWeightedDouble::combiner).average();
  }

  /**
   * Calculates the weighted average of the results.
   *
   * @return the weighted average of the numbers
   */
  public static <U extends Comparable<U> & Serializable> SortedMap<U, Double> weightedAverage(
      MapAggregator<U, ? extends WeightedValue> mr) {
    var result = new TreeMap<U, Double>();
    mr.reduce(
        MutableWeightedDouble::identitySupplier,
        MutableWeightedDouble::accumulator,
        MutableWeightedDouble::combiner).forEach((k, v) -> result.put(k, v.average()));
    return result;
  }

  /**
   * Calculates the averages of the results.
   *
   * <p>
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime
   * exception will be thrown.
   * </p>
   *
   * @return the average of the current data
   */
  public static double average(MapReducer<? extends Number> mr) {
    return mr.map(WeightedValue::new).aggregate(Agg::weightedAverage);
  }

  public static <U extends Comparable<U> & Serializable> SortedMap<U, Double> average(
      MapAggregator<U, ? extends Number> mr) {
    return mr.map(WeightedValue::new).aggregate(Agg::weightedAverage);
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
