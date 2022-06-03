package org.heigit.ohsome.oshdb.api.mapreducer.base;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase.Grouping;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
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
  private final List<Collection<? extends Comparable>> zerofill;

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
      SerializableBiFunction<X, Object, U> indexer, Collection<U> zerofill) {
    this.mapReducer =
        mapReducer.map((data, root) -> new IndexValuePair<>(indexer.apply(data, root), data));
    this.zerofill = new ArrayList<>(1);
    this.zerofill.add(zerofill);
  }

  // "copy/transform" constructor
  MapAggregatorBase(MapAggregatorBase<U, ?> obj,
      MapReducerBase<IndexValuePair<U, X>> mapReducer) {
    this.mapReducer = mapReducer;
    this.zerofill = new ArrayList<>(obj.zerofill);
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

  public <V extends Comparable<V> & Serializable>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
        SerializableFunction<X, V> indexer, Collection<V> zerofill) {
    MapAggregatorBase<OSHDBCombinedIndex<U, V>, X> res =
        this.mapIndex((indexData, ignored) -> new OSHDBCombinedIndex<>(indexData.getKey(),
            indexer.apply(indexData.getValue())));
    res.zerofill.add(zerofill);
    return res;
  }

  // Some internal methods can also aggregate using the "root" object of the mapreducer's view.
  private <V extends Comparable<V> & Serializable>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
        SerializableBiFunction<X, Object, V> indexer, Collection<V> zerofill) {
    MapAggregatorBase<OSHDBCombinedIndex<U, V>, X> res =
        this.mapIndex((indexData, root) -> new OSHDBCombinedIndex<>(indexData.getKey(),
            indexer.apply(indexData.getValue(), root)));
    res.zerofill.add(zerofill);
    return res;
  }

  @Override
  @Contract(pure = true)
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
    return this.aggregateBy(indexer, this.mapReducer.getZerofillTimestamps());
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
      if (aggregationTimestamp == null || aggregationTimestamp.compareTo(minTime) < 0
          || aggregationTimestamp.compareTo(maxTime) > 0) {
        throw new OSHDBInvalidTimestampException(
            "Aggregation timestamp outside of time query interval.");
      }
      return timestamps.floor(aggregationTimestamp);
    }, this.mapReducer.getZerofillTimestamps());
  }

  @Override
  @Contract(pure = true)
  public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(
      Map<V, P> geometries) throws UnsupportedOperationException {
    if (this.mapReducer.grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "aggregateByGeometry() cannot be used together with the groupByEntity() functionality");
    }

    GeometrySplitter<V> gs = new GeometrySplitter<>(geometries);

    MapAggregator<OSHDBCombinedIndex<U, V>, ? extends OSHDBMapReducible> ret;
    if (mapReducer.isOSMContributionViewQuery()) {
      ret = this
          .flatMap((ignored, root) -> gs.splitOSMContribution((OSMContribution) root).entrySet())
          .aggregateBy(Entry::getKey, geometries.keySet()).map(Entry::getValue);
    } else if (mapReducer.isOSMEntitySnapshotViewQuery()) {
      ret = this
          .flatMap(
              (ignored, root) -> gs.splitOSMEntitySnapshot((OSMEntitySnapshot) root).entrySet())
          .aggregateBy(Entry::getKey, geometries.keySet()).map(Entry::getValue);
    } else {
      throw new UnsupportedOperationException(
          String.format(MapReducerBase.UNIMPLEMENTED_DATA_VIEW, this.mapReducer.viewClass));
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

  @Override
  @Contract(pure = true)
  public MapAggregator<U, X> areaOfInterest(OSHDBBoundingBox bboxFilter) {
    return this.copyTransform(this.mapReducer.areaOfInterest(bboxFilter));
  }

  @Override
  @Contract(pure = true)
  public <P extends Geometry & Polygonal> MapAggregator<U, X> areaOfInterest(P polygonFilter) {
    return this.copyTransform(this.mapReducer.areaOfInterest(polygonFilter));
  }

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
  @Contract(pure = true)
  public <R> MapAggregator<U, R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper) {
    return this.copyTransform(this.mapReducer.flatMap(inData -> {
      List<IndexValuePair<U, R>> outData = new LinkedList<>();
      flatMapper.apply(inData.getValue()).forEach(
          flatMappedData -> outData.add(new IndexValuePair<>(inData.getKey(), flatMappedData)));
      return outData;
    }));
  }

  // Some internal methods can also flatMap the "root" object of the mapreducer's view.
  private <R> MapAggregator<U, R> flatMap(
      SerializableBiFunction<X, Object, Iterable<R>> flatMapper) {
    return this.copyTransform(this.mapReducer.flatMap((inData, root) -> {
      List<IndexValuePair<U, R>> outData = new LinkedList<>();
      flatMapper.apply(inData.getValue(), root).forEach(
          flatMappedData -> outData.add(new IndexValuePair<>(inData.getKey(), flatMappedData)));
      return outData;
    }));
  }

  @Override
  @Contract(pure = true)
  public MapAggregator<U, X> filter(SerializablePredicate<X> f) {
    return this.copyTransform(this.mapReducer.filter(data -> f.test(data.getValue())));
  }

  @Override
  @Contract(pure = true)
  public MapAggregator<U, X> filter(FilterExpression f) {
    return this.copyTransform(this.mapReducer.filter(f));
  }

  @Override
  @Contract(pure = true)
  public MapAggregator<U, X> filter(String f) {
    return this.copyTransform(this.mapReducer.filter(f));
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
    // fill nodata entries with "0"
    @SuppressWarnings("unchecked") // all zerofills must "add up" to <U>
    Collection<U> allZerofills =
        (Collection<U>) this.completeZerofill(result.keySet(), Lists.reverse(this.zerofill));
    allZerofills.forEach(zerofillKey -> {
      if (!result.containsKey(zerofillKey)) {
        result.put(zerofillKey, identitySupplier.get());
      }
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

  // calculate complete set of indices to use for zerofilling
  @SuppressWarnings("rawtypes")
  // called recursively: the exact types of the zerofills are not known at this point
  private Collection<? extends Comparable> completeZerofill(Set<? extends Comparable> keys,
      List<Collection<? extends Comparable>> zerofills) {
    if (zerofills.isEmpty()) {
      return Collections.emptyList();
    }
    SortedSet<Comparable> seen = new TreeSet<>(zerofills.get(0));
    SortedSet<Comparable> nextLevelKeys = new TreeSet<>();
    for (Comparable index : keys) {
      Comparable v;
      if (index instanceof OSHDBCombinedIndex) {
        v = ((OSHDBCombinedIndex) index).getSecondIndex();
        nextLevelKeys.add(((OSHDBCombinedIndex) index).getFirstIndex());
      } else {
        v = index;
      }
      seen.add(v);
    }
    if (zerofills.size() == 1) {
      return seen;
    } else {
      Collection<? extends Comparable> nextLevel =
          this.completeZerofill(nextLevelKeys, zerofills.subList(1, zerofills.size()));
      @SuppressWarnings("unchecked") // we don't know the exact types of u and v at this point
      Stream<OSHDBCombinedIndex> combinedZerofillIndices =
          nextLevel.stream().flatMap(u -> seen.stream().map(v -> new OSHDBCombinedIndex(u, v)));
      return combinedZerofillIndices.collect(Collectors.toList());
    }
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
