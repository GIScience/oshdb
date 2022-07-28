package org.heigit.ohsome.oshdb.api.mapreducer.reduction;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorBase;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;

public class Reduce {
  private Reduce() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static Double sum(MapReducerBase<? extends Number> mr) {
    return mr.reduce(() -> 0.0, (p, n) -> p +  n.doubleValue(), Double::sum);
  }

  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static <U> Map<U, Double> sum(MapAggregatorBase<U, ? extends Number> ma) {
    return ma.reduce(() -> 0.0, (p, n) -> p +  n.doubleValue(), Double::sum);
  }


  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static Long sumInt(MapReducerBase<? extends Number> mr) {
    return mr.reduce(() -> 0L, (p, n) -> p +  n.longValue(), Long::sum);
  }

  /**
   * Sums up the results.
   *
   * @return the sum of the current data
   */
  public static <U> Map<U, Long> sumInt(MapAggregatorBase<U, ? extends Number> ma) {
    return ma.reduce(() -> 0L, (p, n) -> p +  n.longValue(), Long::sum);
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
  public static <T> Set<T> uniq(MapReducerBase<T> mr) {
    return mr.reduce(Reduce::uniqIdentitySupplier, Reduce::uniqAccumulator, Reduce::uniqCombiner);
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
  public static <U, T extends Serializable> Map<U, Set<T>> uniq(
      MapAggregatorBase<U, T> mr) {
    return mr.reduce(Reduce::uniqIdentitySupplier, Reduce::uniqAccumulator, Reduce::uniqCombiner);
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
  public static <T> Integer countUniq(MapReducerBase<T> mr) {
    return mr.reduce(Reduce::uniq).size();
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
  public static <U extends Serializable, T extends Serializable> Map<U, Integer> countUniq(
      MapAggregatorBase<U, T> mr) {
    var result = new HashMap<U, Integer>();
    mr.reduce(Reduce::uniq).forEach((k, v) -> result.put(k, v.size()));
    return result;
  }

  /**
   * Calculates the weighted average of the results.
   *
   * @return the weighted average of the numbers
   */
  public static double weightedAverage(MapReducerBase<? extends WeightedValue> mr) {
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
  public static <U extends Serializable> Map<U, Double> weightedAverage(
      MapAggregatorBase<U, ? extends WeightedValue> mr) {
    var result = new HashMap<U, Double>();
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
  public static double average(MapReducerBase<? extends Number> mr) {
    return mr.map(WeightedValue::new).reduce(Reduce::weightedAverage);
  }

  public static <U extends Serializable> Map<U, Double> average(
      MapAggregatorBase<U, ? extends Number> mr) {
    return mr.map(WeightedValue::new).reduce(Reduce::weightedAverage);
  }

  private static <T> Set<T> uniqIdentitySupplier() {
    return new HashSet<>();
  }

  private static <T> Set<T> uniqAccumulator(Set<T> acc, T cur) {
    acc.add(cur);
    return acc;
  }

  private static <T> Set<T> uniqCombiner(Set<T> a, Set<T> b) {
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
