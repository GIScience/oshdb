package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializablePredicate;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer.Grouping;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.jetbrains.annotations.Contract;

import java.util.*;


/**
 * A special variant of a MapAggregator, with improved handling of timestamp-based aggregation:
 *
 * It automatically fills timestamps with no data with "zero"s (which for example results in 0's in the case os `sum()` or `count()`, or `NaN` when using `average()`).
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added mapper function will be called with a parameter of this type as input
 */
public class MapAggregatorByTimestamps<X> extends MapAggregator<OSHDBTimestamp, X> implements
    Mappable<X>, MapAggregatable<MapAggregatorByTimestampAndIndex<? extends Comparable<?>, X>, X>
{
  private boolean _zerofill = true;

  /**
   * basic constructor
   *
   * @param mapReducer mapReducer object which will be doing all actual calculations
   * @param indexer function that returns the timestamp value into which to aggregate the respective result
   */
  MapAggregatorByTimestamps(MapReducer<X> mapReducer, SerializableFunction<X, OSHDBTimestamp> indexer) {
    super(mapReducer, indexer);
  }

  // "copy/transform" constructor
  private MapAggregatorByTimestamps(MapAggregatorByTimestamps<?> obj, MapReducer<Pair<OSHDBTimestamp, X>> mapReducer) {
    this._mapReducer = mapReducer;
    this._zerofill = obj._zerofill;
  }

  @Override
  @Contract(pure = true)
  protected <R> MapAggregatorByTimestamps<R> copyTransform(MapReducer<Pair<OSHDBTimestamp, R>> mapReducer) {
    return new MapAggregatorByTimestamps<>(this, mapReducer);
  }

  /**
   * Enables/Disables the zero-filling feature of otherwise empty timestamp entries in the result.
   *
   * This feature is enabled by default, and can be disabled by calling this function with a value of `false`.
   *
   * @param zerofill the enabled/disabled state of the zero-filling feature
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregatorByTimestamps<X> zerofill(boolean zerofill) {
    MapAggregatorByTimestamps<X> ret = this.copyTransform(this._mapReducer);
    ret._zerofill = zerofill;
    return ret;
  }

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @return a modified copy of this MapAggregatorByTimestamps object operating on the transformed type (&lt;R&gt;)
   */
  @Override
  @Contract(pure = true)
  public <R> MapAggregatorByTimestamps<R> map(SerializableFunction<X, R> mapper) {
    return (MapAggregatorByTimestamps<R>)super.map(mapper);
  }

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number of results per input data entry.
   * The results of this function will be "flattened", meaning that they can be for example transformed again by setting additional `map` functions.
   *
   * @return a modified copy of this MapAggregatorByTimestamps object operating on the transformed type (&lt;R&gt;)
   */
  @Override
  @Contract(pure = true)
  public <R> MapAggregatorByTimestamps<R> flatMap(SerializableFunction<X, List<R>> flatMapper) {
    return (MapAggregatorByTimestamps<R>)super.flatMap(flatMapper);
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @return a modified copy of this MapAggregatorByTimestamps object (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapAggregatorByTimestamps<X> filter(SerializablePredicate<X> f) {
    return (MapAggregatorByTimestamps<X>)super.filter(f);
  }

  /**
   * Map-reduce routine with built-in aggregation by timestamp
   *
   * This can be used to perform an arbitrary map-reduce routine whose results should be aggregated separately according to timestamps.
   *
   * Timestamps with no results are filled with zero values (as provided by the `identitySupplier` function).
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function: `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a> interface.
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;) and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function must be pure (have no side effects), and is not allowed to alter the state of the two input objects it gets!</b>
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   * @throws Exception
   */
  @Override
  @Contract(pure = true)
  public <S> SortedMap<OSHDBTimestamp, S> reduce(SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    SortedMap<OSHDBTimestamp, S> result = super.reduce(identitySupplier, accumulator, combiner);
    if (!this._zerofill) return result;
    // fill nodata entries with "0"
    final SortedSet<OSHDBTimestamp> timestamps = new TreeSet<>(this._mapReducer._tstamps.get());
    // pop last element from timestamps list if we're dealing with OSMContributions (where the
    // timestamps list defines n-1 time intervals)
    if (this._mapReducer._forClass.equals(OSMContribution.class))
      timestamps.remove(timestamps.last());
    timestamps.forEach(ts -> result.putIfAbsent(ts, identitySupplier.get()));
    return result;
  }

  /**
   * Aggregates the results by a second index as well, in addition to the timestamps.
   *
   * @param indexer a function the returns the values that should be used as an additional index on
   *        the aggregated results
   * @param <U> the (arbitrary) data type of this index
   * @return a special MapAggregator object that performs aggregation by two separate indices
   */
  @Contract(pure = true)
  public <U extends Comparable<U>> MapAggregatorByTimestampAndIndex<U, X> aggregateBy(
      SerializableFunction<X, U> indexer
  ) {
    return new MapAggregatorByTimestampAndIndex<U, X>(this, indexer)
        .zerofillTimestamps(this._zerofill);
  }

  /**
   * Aggregates the results by sub-regions as well, in addition to the timestamps.
   *
   * Cannot be used together with the `groupByEntity()` setting enabled.
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has been
   *         activated
   * @throws UnsupportedOperationException when called after any map or flatMap functions are set
   */
  @Contract(pure = true)
  public <U extends Comparable<U>, P extends Geometry & Polygonal>
  MapAggregatorByTimestampAndIndex<U, X> aggregateByGeometry(Map<U, P> geometries) throws
      UnsupportedOperationException
  {
    if (this._mapReducer._grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "aggregateByGeometry() cannot be used together with the groupByEntity() functionality"
      );
    }

    GeometrySplitter<U> gs = new GeometrySplitter<>(geometries);
    if (this._mapReducer._mappers.size() > 1) {
      throw new UnsupportedOperationException(
          "please call aggregateByGeometry before setting any map or flatMap functions"
      );
    } else {
      MapAggregatorByTimestampAndIndex<U, ? extends OSHDBMapReducible> ret;
      if (this._mapReducer._forClass.equals(OSMContribution.class)) {
        ret = this.flatMap(x -> gs.splitOSMContribution((OSMContribution) x))
            .aggregateBy(Pair::getKey).map(Pair::getValue);
      } else if (this._mapReducer._forClass.equals(OSMEntitySnapshot.class)) {
        ret = this.flatMap(x -> gs.splitOSMEntitySnapshot((OSMEntitySnapshot) x))
            .aggregateBy(Pair::getKey).map(Pair::getValue);
      } else {
        throw new UnsupportedOperationException(
            "aggregateByGeometry not implemented for objects of type: " + this._mapReducer._forClass.toString()
        );
      }
      ret = ret.zerofillIndices(geometries.keySet());
      //noinspection unchecked – this._mappers.size() is 0, so the type is still X
      return (MapAggregatorByTimestampAndIndex<U, X>) ret;
    }
  }
}
