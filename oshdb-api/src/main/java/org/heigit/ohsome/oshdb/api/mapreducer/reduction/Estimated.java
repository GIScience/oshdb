package org.heigit.ohsome.oshdb.api.mapreducer.reduction;

import static com.google.common.collect.Streams.stream;
import static java.util.stream.Collectors.toList;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.DoubleUnaryOperator;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorBase;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;

public class Estimated {
  private Estimated() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Returns an estimate of the median of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @return estimated median
   */
  public static double median(MapReducerBase<? extends Number> mr) {
    return mr.reduce(Estimated::digest).quantile(0.5);
  }

  /**
   * Returns an estimate of the median of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @return estimated median
   */
  public static <U extends Comparable<U> & Serializable> SortedMap<U, Double>
      median(MapAggregatorBase<U, ? extends Number> mr) {
    var result = new TreeMap<U, Double>();
    mr.reduce(Estimated::digest).forEach((k, v) -> result.put(k, v.quantile(0.5)));
    return result;
  }

  /**
   * Returns an estimate of a requested quantile of the results after applying the given map
   * function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param q the desired quantile to calculate (as a number between 0 and 1)
   * @return an Aggergator for estimated quantile boundary
   */
  public static double quantile(MapReducerBase<? extends Number> mr, double q) {
    return mr.reduce(Estimated::quantiles).applyAsDouble(q);
  }

  /**
   * Returns an estimate of a requested quantile of the results after applying the given map
   * function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param q the desired quantile to calculate (as a number between 0 and 1)
   * @return an Aggergator for estimated quantile boundary
   */
  public static <U extends Comparable<U> & Serializable> SortedMap<U, Double>
      quantile(MapAggregatorBase<U, ? extends Number> mr, double q) {
    var result = new TreeMap<U, Double>();
    mr.reduce(Estimated::quantiles).forEach((k, v) -> result.put(k, v.applyAsDouble(q)));
    return result;
  }

  /**
   * Returns a function that computes estimates of arbitrary quantiles of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @return a function that computes estimated quantile boundaries
   */
  public static DoubleUnaryOperator quantiles(MapReducerBase<? extends Number> mr) {
    return mr.reduce(Estimated::digest)::quantile;
  }

  /**
   * Returns a function that computes estimates of arbitrary quantiles of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @return a function that computes estimated quantile boundaries
   */
  public static <U extends Comparable<U> & Serializable> SortedMap<U, DoubleUnaryOperator>
      quantiles(MapAggregatorBase<U, ? extends Number> mr) {
    var result = new TreeMap<U, DoubleUnaryOperator>();
    mr.reduce(Estimated::digest).forEach((k, v) -> result.put(k, v::quantile));
    return result;
  }

  /**
   * Returns an estimate of the quantiles of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param qs the desired quantiles to calculate (as a collection of numbers between 0 and 1)
   * @return Aggregator for estimated quantile boundaries
   */
  public static List<Double> quantiles(MapReducerBase<? extends Number> mr, Iterable<Double> qs) {
    var digest = mr.reduce(Estimated::digest);
    return stream(qs).map(digest::quantile).collect(toList());
  }

  /**
   * Returns an estimate of the quantiles of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param qs the desired quantiles to calculate (as a collection of numbers between 0 and 1)
   * @return Aggregator for estimated quantile boundaries
   */
  public static <U extends Comparable<U> & Serializable> SortedMap<U, List<Double>> quantiles(
      MapAggregatorBase<U, ? extends Number> mr, Iterable<Double> qs) {
    var result = new TreeMap<U, List<Double>>();
    mr.reduce(Estimated::digest)
        .forEach((k, v) -> result.put(k, stream(qs).map(v::quantile).collect(toList())));
    return result;
  }

  /**
   * Generates the t-digest of the complete result set. see:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   */
  public static TDigest digest(MapReducerBase<? extends Number> mr) {
    return mr.reduce(
        TdigestReducer::identitySupplier,
        TdigestReducer::accumulator,
        TdigestReducer::combiner);
  }

  /**
   * Generates the t-digest of the complete result set. see:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   */
  public static <U extends Serializable> Map<U, TDigest>
      digest(MapAggregatorBase<U, ? extends Number> mr) {
    return mr.reduce(
        TdigestReducer::identitySupplier,
        TdigestReducer::accumulator,
        TdigestReducer::combiner);
  }

  private static class TdigestReducer {
    private TdigestReducer() {}

    /**
     * A COMPRESSION parameter of 1000 should provide relatively precise results, while not being
     * too demanding on memory usage. See page 20 in the paper [1]:
     *
     * <quote>
     *   &gt; Compression parameter (1/δ) was […] 1000 in order to reliably achieve 0.1% accuracy
     * </quote>
     *
     * <ul><li>
     *   [1] https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
     * </li></ul>
     */
    private static final int COMPRESSION = 1000;

    static TDigest identitySupplier() {
      return new MergingDigest(COMPRESSION);
    }

    static <R extends Number> TDigest accumulator(TDigest acc, R cur) {
      acc.add(cur.doubleValue(), 1);
      return acc;
    }

    static TDigest combiner(TDigest a, TDigest b) {
      if (a.size() == 0) {
        return b;
      } else if (b.size() == 0) {
        return a;
      }
      MergingDigest r = new MergingDigest(COMPRESSION);
      r.add(Arrays.asList(a, b));
      return r;
    }
  }
}