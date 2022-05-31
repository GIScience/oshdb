package org.heigit.ohsome.oshdb.api.mapreducer;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableConsumer;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampList;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public interface MapReducer<X> extends Serializable {
  boolean isCancelable();

  @Contract(pure = true)
  MapReducer<X> keytables(OSHDBJdbc keytables);

  @SuppressWarnings("unused")
  @Contract(pure = true)
  MapReducer<X> tagInterpreter(TagInterpreter tagInterpreter);

  /**
   * Set the area of interest to the given bounding box. Only objects inside or clipped by this bbox
   * will be passed on to the analysis' `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  MapReducer<X> areaOfInterest(@NotNull OSHDBBoundingBox bboxFilter);

  /**
   * Set the area of interest to the given polygon. Only objects inside or clipped by this polygon
   * will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  <P extends Geometry & Polygonal> MapReducer<X> areaOfInterest(@NotNull P polygonFilter);

  @Contract(pure = true)
  MapReducer<X> timestamps(OSHDBTimestampList tstamps);

  @Contract(pure = true)
  MapReducer<X> timestamps(String isoDateStart, String isoDateEnd,
      OSHDBTimestamps.Interval interval);

  @Contract(pure = true)
  MapReducer<X> timestamps(String isoDate);

  @Contract(pure = true)
  MapReducer<X> timestamps(String isoDateStart, String isoDateEnd);

  @Contract(pure = true)
  MapReducer<X> timestamps(String isoDateFirst, String isoDateSecond, String... isoDateMore);

  @Contract(pure = true)
  MapReducer<X> osmTypeInternal(Set<OSMType> typeFilter);

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @param mapper function that will be applied to each data entry (osm entity snapshot or
   *        contribution)
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of the current "Mappable" object operating on the transformed type
   *         (&lt;R&gt;)
   */
  <R> MapReducer<R> map(SerializableFunction<X, R> mapper);

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number
   * of results per input data entry. The results of this function will be "flattened", meaning that
   * they can be for example transformed again by setting additional `map` functions.
   *
   * @param flatMapper function that will be applied to each data entry (osm entity snapshot or
   *        contribution) and returns a list of results
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of the current "Mappable" object operating on the transformed type
   *         (&lt;R&gt;)
   */
  <R> MapReducer<R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper);

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on (when f
   *        returns true) or discarded (when f returns false)
   * @return a modified copy of this "Mappable" (can be used to chain multiple commands together)
   */
  MapReducer<X> filter(SerializablePredicate<X> f);

  /**
   * Apply a custom filter expression to this query.
   *
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#readme">oshdb-filter
   *      readme</a> and {@link org.heigit.ohsome.oshdb.filter} for further information about how to
   *      create such a filter expression object.
   *
   * @param f the {@link org.heigit.ohsome.oshdb.filter.FilterExpression} to apply
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  MapReducer<X> filter(FilterExpression f);

  /**
   * Apply a textual filter to this query.
   *
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#syntax">oshdb-filter
   *      readme</a> for a description of the filter syntax.
   *
   * @param f the filter string to apply
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  MapReducer<X> filter(String f);

  @Contract(pure = true)
  MapReducer<List<X>> groupByEntity() throws UnsupportedOperationException;

  @Contract(pure = true)
  <U extends Comparable<U> & Serializable> MapAggregator<U, X> aggregateBy(
      SerializableFunction<X, U> indexer, Collection<U> zerofill);

  /**
   * Sets a custom aggregation function that is used to (further) group output results into.
   *
   * @param indexer a function that will be called for each input element and returns a value that
   *        will be used to group the results by
   * @param <U> data type of the values used to aggregate the output. has to be a comparable type
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   */
  <U extends Comparable<U> & Serializable> MapAggregator<U, X> aggregateBy(
      SerializableFunction<X, U> indexer);

  @Contract(pure = true)
  MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp() throws UnsupportedOperationException;

  MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) throws UnsupportedOperationException;

  @Contract(pure = true)
  <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal> MapAggregator<U, X> aggregateByGeometry(
      Map<U, P> geometries) throws UnsupportedOperationException;

  /**
   * Generic Map-reduce routine.
   *
   * <p>
   * This can be used to perform an arbitrary reduce routine
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
  <S> S reduce(SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception;

  /**
   * Generic map-reduce routine (shorthand syntax).
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
   * <li>the accumulator function needs to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the accumulator
   * function: `accumulator(identitySupplier(),x)` must be equal to `x`,</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's <a href=
   * "https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(T,java.util.function.BinaryOperator)">reduce(identity,accumulator)</a>
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
  X reduce(SerializableSupplier<X> identitySupplier, SerializableBinaryOperator<X> accumulator)
      throws Exception;

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
  Number sum() throws Exception;

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
  <R extends Number> R sum(SerializableFunction<X, R> mapper) throws Exception;

  /**
   * Counts the number of results.
   *
   * @return the total count of features or modifications, summed up over all timestamps
   */
  Integer count() throws Exception;

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
  Set<X> uniq() throws Exception;

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
  <R> Set<R> uniq(SerializableFunction<X, R> mapper) throws Exception;

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
  Integer countUniq() throws Exception;

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
  Double average() throws Exception;

  /**
   * Calculates the average of the results provided by a given `mapper` function.
   *
   * @param mapper function that returns the numbers to average
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the average of the numbers returned by the `mapper` function
   */
  <R extends Number> Double average(SerializableFunction<X, R> mapper) throws Exception;

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
  Double weightedAverage(SerializableFunction<X, WeightedValue> mapper) throws Exception;

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
  Double estimatedMedian() throws Exception;

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
  <R extends Number> Double estimatedMedian(SerializableFunction<X, R> mapper) throws Exception;

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
  Double estimatedQuantile(double q) throws Exception;

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
  <R extends Number> Double estimatedQuantile(SerializableFunction<X, R> mapper, double q)
      throws Exception;

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
  List<Double> estimatedQuantiles(Iterable<Double> q) throws Exception;

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
  <R extends Number> List<Double> estimatedQuantiles(SerializableFunction<X, R> mapper,
      Iterable<Double> q) throws Exception;

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
  DoubleUnaryOperator estimatedQuantiles() throws Exception;

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
  <R extends Number> DoubleUnaryOperator estimatedQuantiles(SerializableFunction<X, R> mapper)
      throws Exception;

  @Deprecated
  @SuppressWarnings("ResultOfMethodCallIgnored")
  void forEach(SerializableConsumer<X> action) throws Exception;

  /**
   * Collects all results into List(s).
   *
   * @return list(s) with all results returned by the `mapper` function
   */
  List<X> collect() throws Exception;

  /**
   * Returns all results as a Stream.
   *
   * @return a stream with all results returned by the `mapper` function
   */
  Stream<X> stream() throws Exception;
}
