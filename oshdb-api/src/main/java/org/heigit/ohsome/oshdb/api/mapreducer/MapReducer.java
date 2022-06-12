package org.heigit.ohsome.oshdb.api.mapreducer;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Collector;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public interface MapReducer<X> {

  /**
   * Returns if the current backend can be canceled (e.g. in a query timeout).
   */
  default boolean isCancelable() {
    return false;
  }

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
  <R> MapReducer<R> flatMap(SerializableFunction<X, Stream<R>> flatMapper);

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on (when f
   *        returns true) or discarded (when f returns false)
   * @return a modified copy of this "Mappable" (can be used to chain multiple commands together)
   */
  MapReducer<X> filter(SerializablePredicate<X> f);

  // -----------------------------------------------------------------------------------------------
  // Grouping and Aggregation
  // Sets how the input data is "grouped", or the output data is "aggregated" into separate chunks.
  // -----------------------------------------------------------------------------------------------

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

  /**
   * Sets up automatic aggregation by timestamp.
   *
   * <p>In the OSMEntitySnapshotView, the snapshots' timestamp will be used directly to aggregate
   * results into. In the OSMContributionView, the timestamps of the respective data modifications
   * will be matched to corresponding time intervals (that are defined by the `timestamps` setting
   * here).</p>
   *
   * <p>Cannot be used together with the `groupByEntity()` setting enabled.</p>
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has
   *         been activated
   */
  MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp() throws UnsupportedOperationException;

  /**
   * Sets up aggregation by a custom time index.
   *
   * <p>The timestamps returned by the supplied indexing function are matched to the corresponding
   * time intervals.</p>
   *
   * @param indexer a callback function that return a timestamp object for each given data. Note
   *                that if this function returns timestamps outside of the supplied timestamps()
   *                interval results may be undefined
   * @return a MapAggregator object with the equivalent state (settings,
   *         filters, map function, etc.) of the current MapReducer object
   */
  MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer);

  /**
   * Sets up automatic aggregation by geometries.
   *
   * <p>Cannot be used together with the `groupByEntity()` setting enabled.</p>
   *
   * @param geometries an associated list of polygons and identifiers
   * @param <U> the type of the identifers used to aggregate
   * @param <P> a polygonal geometry type
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   */
  <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal>
      MapAggregator<U, X> aggregateByGeometry(Map<U, P> geometries);

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
  default X reduce(SerializableSupplier<X> identitySupplier,
      SerializableBinaryOperator<X> accumulator) {
    return this.reduce(identitySupplier, accumulator::apply, accumulator);
  }

  default <S> S reduce(Function<MapReducer<X>, S> aggregator) {
    return aggregator.apply(this);
  }

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
      SerializableBiFunction<S, X, S> accumulator,
      SerializableBinaryOperator<S> combiner);

  /**
   * Returns all results as a Stream.
   *
   * @return a stream with all results returned by the `mapper` function
   */
  Stream<X> stream();

  /**
   * Counts the number of results.
   *
   * @return the total count of features or modifications, summed up over all timestamps
   */
  default Long count() {
    return reduce(Agg::count);
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
  default Set<X> uniq() {
    return reduce(Agg::uniq);
  }

  /**
  * Counts all unique values of the results.
  *
  * <p>For example, this can be used together with the OSMContributionView to get the number of
  * unique users editing specific feature types.</p>
  *
  * @return the set of distinct values
  */
  default Integer countUniq() {
    return reduce(Agg::countUniq);
  }

  /**
   * Collects all results into List(s).
   *
   * @return list(s) with all results returned by the `mapper` function
   */
  default List<X> collect() {
    return reduce(Collector::toList);
  }
}
