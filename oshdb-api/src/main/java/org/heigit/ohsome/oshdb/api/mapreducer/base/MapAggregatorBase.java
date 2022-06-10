package org.heigit.ohsome.oshdb.api.mapreducer.base;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.jetbrains.annotations.Contract;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

class MapAggregatorBase<U extends Comparable<U> & Serializable, X>
    implements MapAggregator<U, X> {
  private MapReducerBase<IndexValuePair<U, X>> mapReducer;

  /**
   * Basic constructor.
   *
   * @param mapReducer mapReducer object which will be doing all actual calculations
   * @param indexer function that returns the index value into which to aggregate the respective
   *        result
   * @param zerofill collection of index values that should always be present in the final result
   *        (also if they don't appear in the requested data)
   */
  MapAggregatorBase(MapReducerBase<X> mapReducer,
      SerializableBiFunction<X, Object, U> indexer) {
    this.mapReducer =
        mapReducer.map((data, root) -> new IndexValuePair<>(indexer.apply(data, root), data));
  }

  // "copy/transform" constructor
  MapAggregatorBase(MapAggregatorBase<U, ?> obj,
      MapReducerBase<IndexValuePair<U, X>> mapReducer) {
    this.mapReducer = mapReducer;
  }

  /**
   * Creates new mapAggregator object for a specific mapReducer that already contains an aggregation
   * index.
   *
   * <p>
   * Used internally for returning type safe copies of the current mapAggregator object after
   * map/flatMap/filter operations.
   * </p>
   *
   * @param mapReducer a special mapReducer for use in map-aggregate operations
   * @param <R> type of data to be "mapped"
   * @return the mapAggregator object using the given mapReducer
   */
  @Contract(pure = true)
  private <R> MapAggregator<U, R> copyTransform(MapReducerBase<IndexValuePair<U, R>> mapReducer) {
    return new MapAggregatorBase<>(this, mapReducer);
  }

  @Contract(pure = true)
  private <V extends Comparable<V> & Serializable> MapAggregatorBase<V, X> copyTransformKey(
      MapReducerBase<IndexValuePair<V, X>> mapReducer) {
    @SuppressWarnings("unchecked") // we convert the mapAggregator to a new key type "V"
    MapAggregatorBase<V, ?> transformedMapAggregator = (MapAggregatorBase<V, ?>) this;
    return new MapAggregatorBase<>(transformedMapAggregator, mapReducer);
  }

  // ---------------------------------------------------------------------------------------------
  // MapAggregator specific methods
  // ---------------------------------------------------------------------------------------------

  @Override
  public <V extends Comparable<V> & Serializable>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
        SerializableFunction<X, V> indexer) {
    return this.mapIndex((indexData, ignored) -> new OSHDBCombinedIndex<>(indexData.getKey(),
            indexer.apply(indexData.getValue())));
  }

  // Some internal methods can also aggregate using the "root" object of the mapreducer's view.
  private <V extends Comparable<V> & Serializable>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
        SerializableBiFunction<X, Object, V> indexer) {
    return this.mapIndex((indexData, root) -> new OSHDBCombinedIndex<>(indexData.getKey(),
            indexer.apply(indexData.getValue(), root)));
  }

  @Override
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
    // by timestamp indexing function -> for some views we need to match the input data to the
    // list
    SerializableBiFunction<X, Object, OSHDBTimestamp> indexer;
    if (this.mapReducer.isOSMContributionViewQuery()) {
      final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this.mapReducer.tstamps.get());
      indexer = (ignored, root) -> timestamps.floor(((OSMContribution) root).getTimestamp());
    } else if (this.mapReducer.isOSMEntitySnapshotViewQuery()) {
      indexer = (ignored, root) -> ((OSMEntitySnapshot) root).getTimestamp();
    } else {
      throw new UnsupportedOperationException("automatic aggregateByTimestamp() only implemented "
          + "for OSMContribution and OSMEntitySnapshot "
          + "-> try using aggregateByTimestamp(customTimestampIndex) instead");
    }
    return this.aggregateBy(indexer);
  }

  @Override
  @Contract(pure = true)
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this.mapReducer.tstamps.get());
    final OSHDBTimestamp minTime = timestamps.first();
    final OSHDBTimestamp maxTime = timestamps.last();
    return this.aggregateBy(data -> {
      // match timestamps to the given timestamp list
      OSHDBTimestamp aggregationTimestamp = indexer.apply(data);
      if (aggregationTimestamp == null
          || aggregationTimestamp.compareTo(minTime) < 0
          || aggregationTimestamp.compareTo(maxTime) > 0) {
        throw new OSHDBInvalidTimestampException(
            "Aggregation timestamp outside of time query interval.");
      }
      return timestamps.floor(aggregationTimestamp);
    });
  }

  @Override
  @Contract(pure = true)
  public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(
      Map<V, P> geometries) throws UnsupportedOperationException {

    GeometrySplitter<V> gs = new GeometrySplitter<>(geometries);

    MapAggregator<OSHDBCombinedIndex<U, V>, ? extends OSHDBMapReducible> ret;
    if (mapReducer.isOSMContributionViewQuery()) {
      ret = this.flatMap((ignored, root) -> gs.splitOSMContribution((OSMContribution) root))
          .aggregateBy(Entry::getKey).map(Entry::getValue);
    } else if (mapReducer.isOSMEntitySnapshotViewQuery()) {
      ret = this.flatMap((ignored, root) -> gs.splitOSMEntitySnapshot((OSMEntitySnapshot) root))
          .aggregateBy(Entry::getKey).map(Entry::getValue);
    } else {
      throw new UnsupportedOperationException(
          String.format(MapReducerBase.UNIMPLEMENTED_DATA_VIEW, this.mapReducer.viewType));
    }
    @SuppressWarnings("unchecked") // no mapper functions have been applied -> the type is still X
    MapAggregator<OSHDBCombinedIndex<U, V>, X> result =
        (MapAggregator<OSHDBCombinedIndex<U, V>, X>) ret;
    return result;
  }

  // ---------------------------------------------------------------------------------------------
  // Filtering methods
  // Just forwards everything to the wrapped MapReducer object
  // ---------------------------------------------------------------------------------------------

  // ---------------------------------------------------------------------------------------------
  // "Quality of life" helper methods to use the map-reduce functionality more directly and easily
  // for typical queries.
  // Available are: sum, count, average, weightedAverage and uniq.
  // Each one can be used to get results aggregated by timestamp, aggregated by a custom index and
  // not aggregated totals.
  // ---------------------------------------------------------------------------------------------


  // ---------------------------------------------------------------------------------------------
  // "Iterator" like helpers (forEach, collect), mostly intended for testing purposes
  // ---------------------------------------------------------------------------------------------

  @Override
  @Contract(pure = true)
  public Stream<Entry<U, X>> stream() {
    return this.mapReducer.stream().map(d -> Map.entry(d.getKey(), d.getValue()));
  }

  // ---------------------------------------------------------------------------------------------
  // "map", "flatMap" transformation methods
  // ---------------------------------------------------------------------------------------------

  @Override
  @Contract(pure = true)
  public <R> MapAggregator<U, R> map(SerializableFunction<X, R> mapper) {
    return this.copyTransform(this.mapReducer.map(inData -> {
      @SuppressWarnings("unchecked")
      // trick/hack to replace mapped values without copying pair objects
      IndexValuePair<U, R> outData = (IndexValuePair<U, R>) inData;
      outData.setValue(mapper.apply(inData.getValue()));
      return outData;
    }));
  }

  @Override
  public <R> MapAggregator<U, R> flatMap(SerializableFunction<X, Stream<R>> flatMapper) {
    return this.copyTransform(this.mapReducer.flatMap(inData ->
      flatMapper.apply(inData.getValue())
          .map(flatMappedData -> new IndexValuePair<>(inData.getKey(), flatMappedData))));
  }

  // Some internal methods can also flatMap the "root" object of the mapreducer's view.
  private <R> MapAggregator<U, R> flatMap(
      SerializableBiFunction<X, Object, Stream<R>> flatMapper) {
    return this.copyTransform(this.mapReducer.flatMap((inData, root) ->
      flatMapper
            .apply(inData.getValue(), root)
            .map(flatMappedData -> new IndexValuePair<>(inData.getKey(), flatMappedData))));
  }

  @Override
  @Contract(pure = true)
  public MapAggregator<U, X> filter(SerializablePredicate<X> f) {
    return this.copyTransform(this.mapReducer.filter(data -> f.test(data.getValue())));
  }

  // ---------------------------------------------------------------------------------------------
  // Exposed generic reduce.
  // Can be used by experienced users of the api to implement complex queries.
  // These offer full flexibility, but are potentially a bit tricky to work with (see javadoc).
  // ---------------------------------------------------------------------------------------------

  @Override
  @Contract(pure = true)
  public <S> SortedMap<U, S> reduce(SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
    SortedMap<U, S> result =
        this.mapReducer.reduce(TreeMap::new, (TreeMap<U, S> m, IndexValuePair<U, X> r) -> {
          m.put(r.getKey(),
              accumulator.apply(m.getOrDefault(r.getKey(), identitySupplier.get()), r.getValue()));
          return m;
        }, (a, b) -> {
          TreeMap<U, S> combined = new TreeMap<>(a);
          for (Map.Entry<U, S> entry : b.entrySet()) {
            combined.merge(entry.getKey(), entry.getValue(), combiner);
          }
          return combined;
        });
    return result;
  }

  @Override
  @Contract(pure = true)
  public SortedMap<U, X> reduce(SerializableSupplier<X> identitySupplier,
      SerializableBinaryOperator<X> accumulator) {
    return this.reduce(identitySupplier, accumulator::apply, accumulator);
  }

  // ---------------------------------------------------------------------------------------------
  // Some helper methods for internal use in the mapReduce functions
  // ---------------------------------------------------------------------------------------------


  // maps from one index type to a different one
  @Contract(pure = true)
  private <V extends Comparable<V> & Serializable> MapAggregatorBase<V, X> mapIndex(
      SerializableBiFunction<IndexValuePair<U, X>, Object, V> keyMapper) {
    return this.copyTransformKey(this.mapReducer.map(
        (inData, root) -> new IndexValuePair<>(keyMapper.apply(inData, root), inData.getValue())));
  }

  /**
   * A generic Pair class for holding index/value pairs.
   */
  private static class IndexValuePair<U, X> {
    private U key;
    protected X value;

    private IndexValuePair(U key, X value) {
      this.key = key;
      this.value = value;
    }

    public U getKey() {
      return key;
    }

    public X getValue() {
      return value;
    }

    public void setValue(X value) {
      this.value = value;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }

    @Override
    public boolean equals(Object other) {
      return this == other
          || other instanceof IndexValuePair && Objects.equals(key, ((IndexValuePair) other).key)
              && Objects.equals(value, ((IndexValuePair) other).value);
    }

    @Override
    public String toString() {
      return "[index=" + key + ", value=" + value + "]";
    }
  }
}
