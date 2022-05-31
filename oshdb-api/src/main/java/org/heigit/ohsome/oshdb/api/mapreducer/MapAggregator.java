package org.heigit.ohsome.oshdb.api.mapreducer;

import com.tdunning.math.stats.TDigest;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.heigit.ohsome.oshdb.util.function.SerializableBiConsumer;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.jetbrains.annotations.Contract;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public interface MapAggregator<U extends Comparable<U> & Serializable, X> {
  /**
   * Sets up aggregation by another custom index.
   *
   * @param indexer a callback function that returns an index object for each given data
   * @param zerofill a collection of values that are expected to be present in the result
   * @return a MapAggregatorByIndex object with the new index applied as well
   */
  <V extends Comparable<V> & Serializable> MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
      SerializableFunction<X, V> indexer, Collection<V> zerofill);

  /**
   * Sets up aggregation by another custom index.
   *
   * @param indexer a callback function that returns an index object for each given data.
   * @param <V> the type of the values used to aggregate
   * @return a MapAggregatorByIndex object with the new index applied as well
   */
  <V extends Comparable<V> & Serializable> MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
      SerializableFunction<X, V> indexer);

  /**
   * Sets up automatic aggregation by timestamp.
   *
   * <p>
   * In the OSMEntitySnapshotView, the snapshots' timestamp will be used directly to aggregate
   * results into. In the OSMContributionView, the timestamps of the respective data modifications
   * will be matched to corresponding time intervals (that are defined by the `timestamps` setting
   * here).
   * </p>
   *
   * @return a MapAggregatorByTimestampAndIndex object with the equivalent state (settings, filters,
   *         map function, etc.) of the current MapReducer object
   */
  MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp();

  /**
   * Sets up aggregation by a custom time index.
   *
   * <p>
   * The timestamps returned by the supplied indexing function are matched to the corresponding time
   * intervals
   * </p>
   *
   * @param indexer a callback function that returns a timestamp object for each given data. Note
   *        that if this function returns timestamps outside of the supplied timestamps() interval
   *        results may be undefined
   * @return a MapAggregatorByTimestampAndIndex object with the equivalent state (settings, filters,
   *         map function, etc.) of the current MapReducer object
   */
  MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer);

  /**
   * Aggregates the results by sub-regions as well, in addition to the timestamps.
   *
   * <p>
   * Cannot be used together with the `groupByEntity()` setting enabled.
   * </p>
   *
   * @param geometries an associated list of polygons and identifiers
   * @param <V> the type of the identifers used to aggregate
   * @param <P> a polygonal geometry type
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has
   *         been activated
   * @throws UnsupportedOperationException when called after any map or flatMap functions are set
   */
  <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal> MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(
      Map<V, P> geometries) throws UnsupportedOperationException;

  /**
   * Set the area of interest to the given bounding box.
   *
   * <p>
   * Only objects inside or clipped by this bbox will be passed on to the analysis' `mapper`
   * function.
   * </p>
   *
   * @param bboxFilter the bounding box to query the data in
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  MapAggregator<U, X> areaOfInterest(OSHDBBoundingBox bboxFilter);

  /**
   * Set the area of interest to the given polygon. Only objects inside or clipped by this polygon
   * will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  <P extends Geometry & Polygonal> MapAggregator<U, X> areaOfInterest(P polygonFilter);

  /**
   * Sums up the results.
   *
   * <p>
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime
   * exception will be thrown.
   * </p>
   *
   * @return the sum of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  SortedMap<U, Number> sum() throws Exception;

  /**
   * Sums up the results provided by a given `mapper` function.
   *
   * <p>
   * This is a shorthand for `.map(mapper).sum()`, with the difference that here the numerical
   * return type of the `mapper` is ensured.
   * </p>
   *
   * @param mapper function that returns the numbers to sum up
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the summed up results of the `mapper` function
   */
  <R extends Number> SortedMap<U, R> sum(SerializableFunction<X, R> mapper) throws Exception;

  /**
   * Counts the number of results.
   *
   * @return the total count of features or modifications, summed up over all timestamps
   */
  SortedMap<U, Integer> count() throws Exception;

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
  SortedMap<U, Set<X>> uniq() throws Exception;

  /**
   * Gets all unique values of the results provided by a given mapper function.
   *
   * <p>
   * This is a shorthand for `.map(mapper).uniq()`.
   * </p>
   *
   * @param mapper function that returns some values
   * @param <R> the type that is returned by the `mapper` function
   * @return a set of distinct values returned by the `mapper` function
   */
  <R> SortedMap<U, Set<R>> uniq(SerializableFunction<X, R> mapper) throws Exception;

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
  SortedMap<U, Integer> countUniq() throws Exception;

  /**
   * Calculates the averages of the results.
   *
   * <p>
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime
   * exception will be thrown.
   * </p>
   *
   * @return the average of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  SortedMap<U, Double> average() throws Exception;

  /**
   * Calculates the average of the results provided by a given `mapper` function.
   *
   * @param mapper function that returns the numbers to average
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the average of the numbers returned by the `mapper` function
   */
  <R extends Number> SortedMap<U, Double> average(SerializableFunction<X, R> mapper)
      throws Exception;

  /**
   * Calculates the weighted average of the results provided by the `mapper` function.
   *
   * <p>
   * The mapper must return an object of the type `WeightedValue` which contains a numeric value
   * associated with a (floating point) weight.
   * </p>
   *
   * @param mapper function that gets called for each entity snapshot or modification, needs to
   *        return the value and weight combination of numbers to average
   * @return the weighted average of the numbers returned by the `mapper` function
   */
  SortedMap<U, Double> weightedAverage(SerializableFunction<X, WeightedValue> mapper)
      throws Exception;

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
  SortedMap<U, Double> estimatedMedian() throws Exception;

  /**
   * Returns an estimate of the median of the results after applying the given map function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the mean for
   * @return estimated median
   */
  <R extends Number> SortedMap<U, Double> estimatedMedian(SerializableFunction<X, R> mapper)
      throws Exception;

  /**
   * Returns an estimate of a requested quantile of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param q the desired quantile to calculate (as a number between 0 and 1)
   * @return estimated quantile boundary
   */
  SortedMap<U, Double> estimatedQuantile(double q) throws Exception;

  /**
   * Returns an estimate of a requested quantile of the results after applying the given map
   * function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the quantile for
   * @param q the desired quantile to calculate (as a number between 0 and 1)
   * @return estimated quantile boundary
   */
  <R extends Number> SortedMap<U, Double> estimatedQuantile(SerializableFunction<X, R> mapper,
      double q) throws Exception;

  /**
   * Returns an estimate of the quantiles of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param q the desired quantiles to calculate (as a collection of numbers between 0 and 1)
   * @return estimated quantile boundaries
   */
  SortedMap<U, List<Double>> estimatedQuantiles(Iterable<Double> q) throws Exception;

  /**
   * Returns an estimate of the quantiles of the results after applying the given map function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the quantiles for
   * @param q the desired quantiles to calculate (as a collection of numbers between 0 and 1)
   * @return estimated quantile boundaries
   */
  <R extends Number> SortedMap<U, List<Double>> estimatedQuantiles(
      SerializableFunction<X, R> mapper, Iterable<Double> q) throws Exception;

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
  SortedMap<U, DoubleUnaryOperator> estimatedQuantiles() throws Exception;

  /**
   * Returns a function that computes estimates of arbitrary quantiles of the results after applying
   * the given map function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the quantiles for
   * @return a function that computes estimated quantile boundaries
   */
  <R extends Number> SortedMap<U, DoubleUnaryOperator> estimatedQuantiles(
      SerializableFunction<X, R> mapper) throws Exception;

  /**
   * Generates the t-digest of the complete result set. see:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   */
  @Contract(pure = true)
  default <R extends Number> SortedMap<U, TDigest> digest(SerializableFunction<X, R> mapper)
      throws Exception {
    return this.map(mapper).reduce(TdigestReducer::identitySupplier, TdigestReducer::accumulator,
        TdigestReducer::combiner);
  }

  /**
   * Iterates over the results of this data aggregation.
   *
   * <p>
   * This method can be handy for testing purposes. But note that since the `action` doesn't produce
   * a return value, it must facilitate its own way of producing output.
   * </p>
   *
   * <p>
   * If you'd like to use such a "forEach" in a non-test use case, use `.collect().forEach()` or
   * `.stream().forEach()` instead.
   * </p>
   *
   * @param action function that gets called for each transformed data entry
   * @deprecated only for testing purposes. use `.collect().forEach()` or `.stream().forEach()`
   *             instead
   */
  @Deprecated
  void forEach(SerializableBiConsumer<U, List<X>> action) throws Exception;

  /**
   * Collects the results of this data aggregation into Lists.
   *
   * @return an aggregated map of lists with all results
   */
  SortedMap<U, List<X>> collect() throws Exception;

  /**
   * Returns all results as a Stream.
   *
   * @return a stream with all results returned by the `mapper` function
   */
  Stream<Map.Entry<U, X>> stream() throws Exception;

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @param mapper function that will be applied to each data entry (osm entity snapshot or
   *        contribution)
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapAggregator object operating on the transformed type R
   */
  <R> MapAggregator<U, R> map(SerializableFunction<X, R> mapper);

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number
   * of results per input data entry.
   *
   * <p>
   * The results of this function will be "flattened", meaning that they can be for example
   * transformed again by setting additional `map` functions.
   * </p>
   *
   * @param flatMapper function that will be applied to each data entry (osm entity snapshot or
   *        contribution) and returns a list of results
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapAggregator object operating on the transformed type R
   */
  <R> MapAggregator<U, R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper);

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on (when f
   *        returns true) or discarded (when f returns false)
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  MapAggregator<U, X> filter(SerializablePredicate<X> f);

  /**
   * Apply a custom filter expression to this query.
   *
   * @param f the {@link org.heigit.ohsome.oshdb.filter.FilterExpression} to apply
   * @return a modified copy of this object (can be used to chain multiple commands together)
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#readme">oshdb-filter
   *      readme</a> and {@link org.heigit.ohsome.oshdb.filter} for further information about how to
   *      create such a filter expression object.
   */
  MapAggregator<U, X> filter(FilterExpression f);

  /**
   * Apply a textual filter to this query.
   *
   * @param f the filter string to apply
   * @return a modified copy of this object (can be used to chain multiple commands together)
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#syntax">oshdb-filter
   *      readme</a> for a description of the filter syntax.
   */
  MapAggregator<U, X> filter(String f);

  /**
   * Map-reduce routine with built-in aggregation.
   *
   * <p>
   * This can be used to perform an arbitrary reduce routine whose results are aggregated separately
   * according to some custom index value.
   * </p>
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's <a href=
   * "https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  <S> SortedMap<U, S> reduce(SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception;

  /**
   * Map-reduce routine with built-in aggregation (shorthand syntax).
   * <p>
   * This can be used to perform an arbitrary reduce routine whose results are aggregated separately
   * according to some custom index value.
   * </p>
   *
   * <p>
   * This variant is shorter to program than `reduce(identitySupplier, accumulator, combiner)`, but
   * can only be used if the result type is the same as the current `map`ped type &lt;X&gt;. Also
   * this variant can be less efficient since it cannot benefit from the mutability freedoms the
   * accumulator+combiner approach has.
   * </p>
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's <a href=
   * "https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;X&gt;)
   *        and an accumulation value (also of type &lt;X&gt;, e.g. the result of
   *        `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this
   *        function is not to alter (mutate) the state of the accumulation value (e.g. directly
   *        adding new values to an existing Set object)
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  SortedMap<U, X> reduce(SerializableSupplier<X> identitySupplier,
      SerializableBinaryOperator<X> accumulator) throws Exception;
}
