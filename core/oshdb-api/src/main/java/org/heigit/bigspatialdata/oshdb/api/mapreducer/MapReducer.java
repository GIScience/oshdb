package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import java.io.Serializable;
import java.util.*;
import java.util.List;
import java.util.function.*;
import java.util.stream.Collectors;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Ignite;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_JDBC;
import org.heigit.bigspatialdata.oshdb.api.generic.NumberUtils;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.*;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

public abstract class MapReducer<T> {
  protected OSHDB _oshdb;
  protected OSHDB_JDBC _oshdbForTags;
  protected Class _forClass = null;
  private BoundingBox _bboxFilter = null;
  private Polygon _polyFilter = null;
  private OSHDBTimestamps _tstamps = null;
  private final List<SerializablePredicate<OSHEntity>> _preFilters = new ArrayList<>();
  private final List<SerializablePredicate<OSMEntity>> _filters = new ArrayList<>();
  protected TagInterpreter _tagInterpreter = null;
  protected TagTranslator _tagTranslator = null;
  protected EnumSet<OSMType> _typeFilter = EnumSet.allOf(OSMType.class);
  
  protected MapReducer(OSHDB oshdb) {
    this._oshdb = oshdb;
  }

  public static <T> MapReducer<T> using(OSHDB oshdb, Class<?> forClass) {
    if (oshdb instanceof OSHDB_JDBC) {
      MapReducer<T> mapper;
      if (((OSHDB_JDBC)oshdb).multithreading())
        mapper = new MapReducer_JDBC_multithread<T>(oshdb);
      else
        mapper = new MapReducer_JDBC_singlethread<T>(oshdb);
      mapper._oshdb = oshdb;
      mapper._oshdbForTags = (OSHDB_JDBC)oshdb;
      mapper._forClass = forClass;
      return mapper;
    } else if (oshdb instanceof OSHDB_Ignite) {
      MapReducer<T> mapper = new MapReducer_Ignite<T>(oshdb);
      mapper._oshdbForTags = null;
      mapper._forClass = forClass;
      return mapper;
    } else {
      throw new UnsupportedOperationException("No mapper implemented for your database type");
    }
  }
  
  public MapReducer<T> keytables(OSHDB_JDBC oshdb) {
    this._oshdbForTags = oshdb;
    return this;
  }

  protected Integer getTagKeyId(String key) throws Exception {
    if (this._tagTranslator == null) this._tagTranslator = new TagTranslator((this._oshdbForTags).getConnection());
    return this._tagTranslator.key2Int(key);
  }

  protected Pair<Integer, Integer> getTagValueId(String key, String value) throws Exception {
    if (this._tagTranslator == null) this._tagTranslator = new TagTranslator((this._oshdbForTags).getConnection());
    return this._tagTranslator.tag2Int(new ImmutablePair(key,value));
  }

  public interface SerializableSupplier<R> extends Supplier<R>, Serializable {}
  public interface SerializablePredicate<T> extends Predicate<T>, Serializable {}
  public interface SerializableBinaryOperator<T> extends BinaryOperator<T>, Serializable {}
  public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}
  public interface SerializableBiFunction<T1, T2, R> extends BiFunction<T1, T2, R>, Serializable {}

  
  protected <R, S> S mapReduceCellsOSMContribution(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, Polygon poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, Polygon poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<List<OSMContribution>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }

  protected <R, S> S mapReduceCellsOSMEntitySnapshot(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, Polygon poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, Polygon poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }


  @Deprecated
  public MapReducer<T> boundingBox(BoundingBox bbox) {
    return this.areaOfInterest(bbox);
  }

  public MapReducer<T> areaOfInterest(BoundingBox bboxFilter) {
    if (this._polyFilter == null) {
      if (this._bboxFilter == null)
        this._bboxFilter = bboxFilter;
      else
        this._bboxFilter = BoundingBox.intersect(bboxFilter, this._bboxFilter);
    } else {
      this._polyFilter = (Polygon)Geo.clip(this._polyFilter, bboxFilter);
      this._bboxFilter = new BoundingBox(this._polyFilter.getEnvelopeInternal());
    }
    return this;
  }
  public MapReducer<T> areaOfInterest(Polygon polygonFilter) {
    if (this._polyFilter == null) {
      if (this._bboxFilter == null)
        this._polyFilter = polygonFilter;
      else
        this._polyFilter = (Polygon)Geo.clip(polygonFilter, this._bboxFilter);
    } else {
      this._polyFilter = (Polygon)Geo.clip(polygonFilter, this._polyFilter);
    }
    this._bboxFilter = new BoundingBox(this._polyFilter.getEnvelopeInternal());
    return this;
  }
  
  public MapReducer<T> timestamps(OSHDBTimestamps tstamps) {
    this._tstamps = tstamps;
    return this;
  }

  public MapReducer<T> osmTypes(EnumSet<OSMType> typeFilter) {
    this._typeFilter = typeFilter;
    return this;
  }

  public MapReducer<T> osmTypes(OSMType type1) {
    return this.osmTypes(EnumSet.of(type1));
  }
  public MapReducer<T> osmTypes(OSMType type1, OSMType type2) {
    return this.osmTypes(EnumSet.of(type1, type2));
  }
  public MapReducer<T> osmTypes(OSMType type1, OSMType type2, OSMType type3) {
    return this.osmTypes(EnumSet.of(type1, type2, type3));
  }
  
  public MapReducer<T> tagInterpreter(TagInterpreter tagInterpreter) {
    this._tagInterpreter = tagInterpreter;
    return this;
  }
  
  public MapReducer<T> filter(SerializablePredicate<OSMEntity> f) {
    this._filters.add(f);
    return this;
  }
  
  @FunctionalInterface
  public interface ExceptionFunction<X, Y> {
    Y apply(X x) throws Exception;
  }
  
  public MapReducer<T> filterByTagKey(String key) throws Exception {
    int keyId = this.getTagKeyId(key);
    this._preFilters.add(oshEntitiy -> oshEntitiy.hasTagKey(keyId));
    this._filters.add(osmEntity -> osmEntity.hasTagKey(keyId));
    return this;
  }
  
  public MapReducer<T> filterByTagValue(String key, String value) throws Exception {
    Pair<Integer, Integer> keyValueId = this.getTagValueId(key, value);
    int keyId = keyValueId.getKey();
    int valueId = keyValueId.getValue();
    this._filters.add(osmEntity -> osmEntity.hasTagValue(keyId, valueId));
    return this;
  }

  private SerializablePredicate<OSHEntity> _getPreFilter() {
    return (this._preFilters.isEmpty()) ? (oshEntity -> true) : (oshEntity -> {
      for (SerializablePredicate<OSHEntity> filter : this._preFilters)
        if (!filter.test(oshEntity))
          return false;
      return true;
    });
  }

  private SerializablePredicate<OSMEntity> _getFilter() {
    return (this._filters.isEmpty()) ? (osmEntity -> true) : (osmEntity -> {
      for (SerializablePredicate<OSMEntity> filter : this._filters)
        if (!filter.test(osmEntity))
          return false;
      return true;
    });
  }

  private Iterable<CellId> _getCellIds() {
    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
    if (this._bboxFilter == null || (this._bboxFilter.minLon >= this._bboxFilter.maxLon || this._bboxFilter.minLat >= this._bboxFilter.maxLat)) {
      // return an empty iterable if bbox is not set or empty
      System.err.println("warning: area of interest not set or empty");
      return Collections.emptyList();
    }
    return grid.bbox2CellIds(this._bboxFilter, true);
  }

  private List<Long> _getTimestamps() {
    return this._tstamps.getTimestamps();
  }
  
  public <R, S> S mapReduce(SerializableFunction<T, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    if (this._forClass.equals(OSMContribution.class)) {
      return this.mapReduceCellsOSMContribution(this._getCellIds(), this._getTimestamps(), this._bboxFilter, this._polyFilter, this._getPreFilter(), this._getFilter(), (SerializableFunction<OSMContribution, R>) mapper, identitySupplier, accumulator, combiner);
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      return this.mapReduceCellsOSMEntitySnapshot(this._getCellIds(), this._getTimestamps(), this._bboxFilter, this._polyFilter, this._getPreFilter(), this._getFilter(), (SerializableFunction<OSMEntitySnapshot, R>) mapper, identitySupplier, accumulator, combiner);
    } else throw new UnsupportedOperationException("No mapper implemented for your database type");
  }

  public <R, S> S flatMapReduceGroupedById(SerializableFunction<List<T>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    if (this._forClass.equals(OSMContribution.class)) {
      return this.flatMapReduceCellsOSMContributionGroupedById(this._getCellIds(), this._getTimestamps(), this._bboxFilter, this._polyFilter, this._getPreFilter(), this._getFilter(), (SerializableFunction<List<OSMContribution>, List<R>>) contributions -> mapper.apply((List<T>)contributions), identitySupplier, accumulator, combiner);
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      return this.flatMapReduceCellsOSMEntitySnapshotGroupedById(this._getCellIds(), this._getTimestamps(), this._bboxFilter, this._polyFilter, this._getPreFilter(), this._getFilter(), (SerializableFunction<List<OSMEntitySnapshot>, List<R>>) contributions -> mapper.apply((List<T>)contributions), identitySupplier, accumulator, combiner);
    } else throw new UnsupportedOperationException("No mapper implemented for your database type");
  }
  public <R, S> S flatMapReduce(SerializableFunction<T, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    return this.flatMapReduceGroupedById(
        (List<T> inputList) -> {
          List<R> outputList = new LinkedList<>();
          inputList.stream().map(mapper).forEach(outputList::addAll);
          return outputList;
        },
        identitySupplier,
        accumulator,
        combiner
    );
  }

  public <R, S, U> SortedMap<U, S> mapAggregate(SerializableFunction<T, Pair<U, R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    return this.mapReduce(mapper, TreeMap::new, (SortedMap<U, S> m, Pair<U, R> r) -> {
      m.put(r.getKey(), accumulator.apply(m.getOrDefault(r.getKey(), identitySupplier.get()), r.getValue()));
      return m;
    }, (a,b) -> {
      SortedMap<U, S> combined = new TreeMap<>(a);
      for (SortedMap.Entry<U, S> entry: b.entrySet()) {
        combined.merge(entry.getKey(), entry.getValue(), combiner);
      }
      return combined;
    });
  }

  public <R, S, U> SortedMap<U, S> flatMapAggregateGroupedById(SerializableFunction<List<T>, List<Pair<U, R>>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    return this.flatMapReduceGroupedById(mapper, TreeMap::new, (SortedMap<U, S> m, Pair<U, R> r) -> {
      m.put(r.getKey(), accumulator.apply(m.getOrDefault(r.getKey(), identitySupplier.get()), r.getValue()));
      return m;
    }, (a,b) -> {
      SortedMap<U, S> combined = new TreeMap<>(a);
      for (SortedMap.Entry<U, S> entry: b.entrySet()) {
        combined.merge(entry.getKey(), entry.getValue(), combiner);
      }
      return combined;
    });
  }

  public <R, S, U> SortedMap<U, S> flatMapAggregate(SerializableFunction<T, List<Pair<U, R>>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    return this.flatMapAggregateGroupedById(
        inputList -> {
          List<Pair<U, R>> outputList = new LinkedList<>();
          inputList.stream().map(mapper).forEach(outputList::addAll);
          return outputList;
        },
        identitySupplier,
        accumulator,
        combiner
    );
  }

  public <R, S> SortedMap<OSHDBTimestamp, S> mapAggregateByTimestamp(SerializableFunction<T, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    SortedMap<OSHDBTimestamp, S> result;
    List<OSHDBTimestamp> timestamps = this._getTimestamps().stream().map(OSHDBTimestamp::new).collect(Collectors.toList());
    if (this._forClass.equals(OSMContribution.class)) {
      result = this.mapAggregate(t -> {
        int timeBinIndex = Collections.binarySearch(timestamps, ((OSMContribution) t).getTimestamp());
        if (timeBinIndex < 0) { timeBinIndex = -timeBinIndex - 2; }
        return new ImmutablePair<>(timestamps.get(timeBinIndex), mapper.apply(t));
      }, identitySupplier, accumulator, combiner);
      timestamps.remove(timestamps.size()-1); // pop last element from timestamps list, so it doesn't get nodata-filled with "0" below
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      result = this.mapAggregate(t -> {
        return new ImmutablePair<>(((OSMEntitySnapshot) t).getTimestamp(), mapper.apply(t));
      }, identitySupplier, accumulator, combiner);
    } else throw new UnsupportedOperationException("mapAggregateByTimestamp only allowed for OSMContribution and OSMEntitySnapshot");
    // fill nodata entries with "0"
    timestamps.forEach(ts -> result.putIfAbsent(ts, identitySupplier.get()));
    return result;
  }

  // default helper methods to use the mapreducer's functionality more easily
  // like sum, weight, average, uniq, etc.

  public <R extends Number> SortedMap<OSHDBTimestamp, R> sumAggregateByTimestamp(SerializableFunction<T, R> mapper) throws Exception {
    return this.mapAggregateByTimestamp(mapper, () -> (R) (Integer) 0, (SerializableBiFunction<R, R, R>)(x,y) -> NumberUtils.add(x,y), (x,y) -> NumberUtils.add(x,y));
  }
  public SortedMap<OSHDBTimestamp, Integer> countAggregateByTimestamp() throws Exception {
    return this.sumAggregateByTimestamp(ignored -> 1);
  }
  public <R extends Number> SortedMap<OSHDBTimestamp, Double> averageAggregateByTimestamp(SerializableFunction<T, R> mapper) throws Exception {
    return this.mapAggregateByTimestamp(
        mapper,
        () -> new PayloadWithWeight<>((R) (Double) 0.0,0),
        (acc, cur) -> {
          acc.num = NumberUtils.add(acc.num, cur);
          acc.weight += 1;
          return acc;
        },
        (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight+b.weight)
    ).entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> e.getValue().num.doubleValue() / e.getValue().weight,
        (v1, v2) -> v1,
        TreeMap::new
    ));
  }
  public <R extends Number, W extends Number> SortedMap<OSHDBTimestamp, Double> weightedAverageAggregateByTimestamp(SerializableFunction<T, Pair<R, W>> mapper) throws Exception {
    return this.mapAggregateByTimestamp(
        mapper,
        () -> new PayloadWithWeight<>((R) (Double) 0.0,0),
        (acc, cur) -> {
          acc.num = NumberUtils.add(acc.num, cur.getLeft());
          acc.weight += cur.getRight().doubleValue();
          return acc;
        },
        (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight+b.weight)
    ).entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> e.getValue().num.doubleValue() / e.getValue().weight,
        (v1, v2) -> v1,
        TreeMap::new
    ));
  }
  public <R> SortedMap<OSHDBTimestamp, Set<R>> uniqAggregateByTimestamp(SerializableFunction<T, R> mapper) throws Exception {
    return this.mapAggregateByTimestamp(
        mapper,
        HashSet<R>::new,
        (acc, cur) -> {
          acc.add(cur);
          return acc;
        },
        (set1, set2) -> {
          Set<R> combinedSets = new HashSet<R>(set1);
          combinedSets.addAll(set2);
          return combinedSets;
        }
    );
  }

  public <R extends Number, U> SortedMap<U, R> sumAggregate(SerializableFunction<T, Pair<U, R>> mapper) throws Exception {
    return this.mapAggregate(mapper, () -> (R) (Integer) 0, (SerializableBiFunction<R, R, R>)(x,y) -> NumberUtils.add(x,y), (x,y) -> NumberUtils.add(x,y));
  }
  public <U> SortedMap<U, Integer> countAggregate(SerializableFunction<T, U> mapper) throws Exception {
    return this.sumAggregate(data -> new ImmutablePair<U, Integer>(mapper.apply(data), 1));
  }
  public <R extends Number, U> SortedMap<U, Double> averageAggregate(SerializableFunction<T, Pair<U, R>> mapper) throws Exception {
    return this.mapAggregate(
        mapper,
        () -> new PayloadWithWeight<>((R) (Double) 0.0,0),
        (acc, cur) -> {
          acc.num = NumberUtils.add(acc.num, cur);
          acc.weight += 1;
          return acc;
        },
        (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight+b.weight)
    ).entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> e.getValue().num.doubleValue() / e.getValue().weight,
        (v1, v2) -> v1,
        TreeMap::new
    ));
  }
  public <R extends Number, W extends Number, U> SortedMap<U, Double> weightedAverageAggregate(SerializableFunction<T, Pair<U, Pair<R, W>>> mapper) throws Exception {
    return this.mapAggregate(
        mapper,
        () -> new PayloadWithWeight<>((R) (Double) 0.0,0),
        (acc, cur) -> {
          acc.num = NumberUtils.add(acc.num, cur.getLeft());
          acc.weight += cur.getRight().doubleValue();
          return acc;
        },
        (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight+b.weight)
    ).entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> e.getValue().num.doubleValue() / e.getValue().weight,
        (v1, v2) -> v1,
        TreeMap::new
    ));
  }
  public <R, U> SortedMap<U, Set<R>> uniqAggregate(SerializableFunction<T, Pair<U, R>> mapper) throws Exception {
    return this.mapAggregate(
        mapper,
        HashSet<R>::new,
        (acc, cur) -> {
          acc.add(cur);
          return acc;
        },
        (set1, set2) -> {
          Set<R> combinedSets = new HashSet<R>(set1);
          combinedSets.addAll(set2);
          return combinedSets;
        }
    );
  }

  public <R extends Number> R sum(SerializableFunction<T, R> mapper) throws Exception {
    return this.mapReduce(mapper, () -> (R) (Integer) 0, (SerializableBiFunction<R, R, R>)(x,y) -> NumberUtils.add(x,y), (SerializableBinaryOperator<R>)(x,y) -> NumberUtils.add(x,y));
  }
  public Integer count() throws Exception {
    return this.sum(ignored -> 1);
  }
  public <R> Set<R> uniq(SerializableFunction<T, R> mapper) throws Exception {
    return this.uniqAggregate(data -> new ImmutablePair<>(0, mapper.apply(data))).getOrDefault(0, new HashSet<>());
  }

  private class PayloadWithWeight<X> {
    X num;
    double weight;
    PayloadWithWeight(X num, double weight) {
      this.num = num;
      this.weight = weight;
    }
  }
}
