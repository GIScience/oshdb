package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializablePredicate;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer.Grouping;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.jetbrains.annotations.Contract;

/**
 * …
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added mapper function will be called with a parameter of this type as input
 */
public class MapAggregatorByIndex<U extends Comparable<U>, X> extends MapAggregator<U, X> implements
    Mappable<X>/*, MapAggregatorByTimestampsSettings<MapAggregatorByIndex<U, X>>, MapAggregatable<MapAggregatorByTimestampAndIndex<U, X>, X>*/
{
  private Collection<U> _zerofill = Collections.emptyList();

  /**
   * basic constructor
   *
   * @param mapReducer mapReducer object which will be doing all actual calculations
   * @param indexer function that returns the timestamp value into which to aggregate the respective result
   */
  MapAggregatorByIndex(MapReducer<X> mapReducer, SerializableFunction<X, U> indexer) {
    super(mapReducer, indexer);
  }

  // "copy/transform" constructor
  private MapAggregatorByIndex(MapAggregatorByIndex<U, ?> obj, MapReducer<Pair<U, X>> mapReducer) {
    this._mapReducer = mapReducer;
    this._zerofill = obj._zerofill;
  }

  @Override
  @Contract(pure = true)
  protected <R> MapAggregatorByIndex<U, R> copyTransform(MapReducer<Pair<U, R>> mapReducer) {
    return new MapAggregatorByIndex<>(this, mapReducer);
  }
  @Contract(pure = true)
  private <V extends Comparable<V>> MapAggregatorByIndex<V, X> copyTransformKey(MapReducer<Pair<V, X>> mapReducer) {
    //noinspection unchecked – we do want to convert the mapAggregator to a different key type "V"
    return new MapAggregatorByIndex<V, X>((MapAggregatorByIndex<V, ?>) this, mapReducer);
  }

  /**
   * …
   */
  @Contract(pure = true)
  private <V extends Comparable<V>> MapAggregatorByIndex<V, X> mapIndex(SerializableBiFunction<U, X, V> keyMapper) {
    return this.copyTransformKey(this._mapReducer.map(inData -> new MutablePair<>(
        keyMapper.apply(inData.getKey(), inData.getValue()),
        inData.getValue()
    )));
  }

  /**
   * Enables/Disables the zero-filling of otherwise empty entries in the result.
   *
   * @param zerofillKeys a collection of keys whose values should be filled with "zeros" if they
   *        would otherwise not be present in the result
   * @return a modified copy of this object (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapAggregatorByIndex<U, X> zerofill(Collection<U> zerofillKeys) {
    MapAggregatorByIndex<U, X> ret = this.copyTransform(this._mapReducer);
    ret._zerofill = zerofillKeys;
    return ret;
  }

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @return a modified copy of this MapAggregatorByTimestamps object operating on the transformed type (&lt;R&gt;)
   */
  @Override
  @Contract(pure = true)
  public <R> MapAggregatorByIndex<U, R> map(SerializableFunction<X, R> mapper) {
    return (MapAggregatorByIndex<U, R>)super.map(mapper);
  }

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number of results per input data entry.
   * The results of this function will be "flattened", meaning that they can be for example transformed again by setting additional `map` functions.
   *
   * @return a modified copy of this MapAggregatorByTimestamps object operating on the transformed type (&lt;R&gt;)
   */
  @Override
  @Contract(pure = true)
  public <R> MapAggregatorByIndex<U, R> flatMap(SerializableFunction<X, List<R>> flatMapper) {
    return (MapAggregatorByIndex<U, R>)super.flatMap(flatMapper);
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @return a modified copy of this MapAggregatorByTimestamps object (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapAggregatorByIndex<U, X> filter(SerializablePredicate<X> f) {
    return (MapAggregatorByIndex<U, X>)super.filter(f);
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
  public <S> SortedMap<U, S> reduce(SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    SortedMap<U, S> result = super.reduce(identitySupplier, accumulator, combiner);
    if (this._zerofill.isEmpty()) return result;
    // fill nodata entries with "0"
    for (U zerofillKey : this._zerofill) {
      if (!result.containsKey(zerofillKey)) {
        result.put(zerofillKey, identitySupplier.get());
      }
    }
    return result;
  }

  /**
   * Sets up aggregation by a custom time index.
   *
   * The timestamps returned by the supplied indexing function are matched to the corresponding
   * time intervals
   *
   * @param indexer a callback function that returns a timestamp object for each given data.
   *                Note that if this function returns timestamps outside of the supplied
   *                timestamps() interval results may be undefined
   * @return a MapAggregatorByTimestampAndIndex object with the equivalent state (settings,
   *         filters, map function, etc.) of the current MapReducer object
   */
  @Contract(pure = true)
  public MapAggregatorByTimestampAndIndex<U, X> aggregateByTimestamp(SerializableFunction<X, OSHDBTimestamp> indexer) {
    final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this._mapReducer._tstamps.get());
    return new MapAggregatorByTimestampAndIndex<U, X>(this, data -> {
      // match timestamps to the given timestamp list
      return timestamps.floor(indexer.apply(data));
    }).zerofillIndices(this._zerofill);
  }

  /**
   * Sets up aggregation by another custom index.
   *
   * @param indexer a callback function that returns an index object for each given data.
   * @return a MapAggregatorByIndex object with the new index applied as well
   */
  @Contract(pure = true)
  public <V extends Comparable<V>> MapAggregatorByIndex<OSHDBCombinedIndex<U, V>, X> aggregateBy(SerializableFunction<X, V> indexer) {
    return this.mapIndex((existingIndex, data) -> new OSHDBCombinedIndex<U, V>(
        existingIndex,
        indexer.apply(data)
    ));
    //todo: .zerofillIndices(this._zerofill);
  }
}
