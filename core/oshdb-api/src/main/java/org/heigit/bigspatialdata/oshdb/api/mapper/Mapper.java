package org.heigit.bigspatialdata.oshdb.api.mapper;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.generic.NumberUtils;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.api.objects.Timestamp;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.utils.TagTranslator;

public abstract class Mapper<T> {
  protected OSHDB _oshdb;
  protected OSHDB _oshdbForTags;
  protected Class _forClass = null;
  private BoundingBox _bbox = null;
  private OSHDBTimestamps _tstamps = null;
  private final List<Predicate<OSHEntity>> _preFilters = new ArrayList<>();
  private final List<Predicate<OSMEntity>> _filters = new ArrayList<>();
  protected TagInterpreter _tagInterpreter = null;
  protected TagTranslator _tagTranslator=null;
  
  protected Mapper(OSHDB oshdb) {
    this._oshdb = oshdb;
  }

  public static <T> Mapper<T> using(OSHDB oshdb, Class<?> forClass) {
    if (oshdb instanceof OSHDB_H2) {
      Mapper<T> mapper;
      if (((OSHDB_H2)oshdb).multithreading())
        mapper = new Mapper_H2_multithread<T>((OSHDB_H2) oshdb);
      else
        mapper = new Mapper_H2_singlethread<T>((OSHDB_H2) oshdb);
      mapper._oshdb = oshdb;
      mapper._oshdbForTags = oshdb;
      mapper._forClass = forClass;
      return mapper;
    } else throw new UnsupportedOperationException("No mapper implemented for your database type");
  }
  
  public Mapper<T> usingForTags(OSHDB oshdb) {
    this._oshdbForTags = oshdb;
    return this;
  }
  
  protected abstract Integer getTagKeyId(String key) throws Exception;
  protected abstract Pair<Integer, Integer> getTagValueId(String key, String value) throws Exception;

  protected <R, S> S reduceCellsOSMContribution(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, Predicate<OSHEntity> preFilter, Predicate<OSMEntity> filter, Function<OSMContribution, R> mapper, Supplier<S> identitySupplier, BiFunction<S, R, S> accumulator, BinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }
  
  protected <R, S> S reduceCellsOSMEntity(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, Predicate<OSHEntity> preFilter, Predicate<OSMEntity> filter, Function<OSMEntity, R> mapper, Supplier<S> identitySupplier, BiFunction<S, R, S> accumulator, BinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }
  
  protected <R, S> S reduceCellsOSMEntitySnapshot(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, Predicate<OSHEntity> preFilter, Predicate<OSMEntity> filter, Function<OSMEntitySnapshot, R> mapper, Supplier<S> identitySupplier, BiFunction<S, R, S> accumulator, BinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }
  
  public Mapper<T> boundingBox(BoundingBox bbox) {
    this._bbox = bbox;
    return this;
  }
  
  public Mapper<T> timestamps(OSHDBTimestamps tstamps) {
    this._tstamps = tstamps;
    return this;
  }
  
  public Mapper<T> tagInterpreter(TagInterpreter tagInterpreter) {
    this._tagInterpreter = tagInterpreter;
    return this;
  }
  
  public Mapper<T> filter(Predicate<OSMEntity> f) {
    this._filters.add(f);
    return this;
  }
  
  @FunctionalInterface
  public interface ExceptionFunction<X, Y> {
    Y apply(X x) throws Exception;
  }
  
  public Mapper<T> filterByTagKey(String key) throws Exception {
    int keyId = this.getTagKeyId(key);
    this._preFilters.add(oshEntitiy -> oshEntitiy.hasTagKey(keyId));
    this._filters.add(osmEntity -> osmEntity.hasTagKey(keyId));
    return this;
  }
  
  public Mapper<T> filterByTagValue(String key, String value) throws Exception {
    Pair<Integer, Integer> keyValueId = this.getTagValueId(key, value);
    int keyId = keyValueId.getKey();
    int valueId = keyValueId.getValue();
    this._filters.add(osmEntity -> osmEntity.hasTagValue(keyId, valueId));
    return this;
  }

  private Predicate<OSHEntity> _getPreFilter() {
    return (this._preFilters.isEmpty()) ? (oshEntity -> true) : this._preFilters.stream().reduce(Predicate::and).get();
  }

  private Predicate<OSMEntity> _getFilter() {
    return (this._filters.isEmpty()) ? (osmEntity -> true) : this._filters.stream().reduce(Predicate::and).get();
  }

  private Iterable<CellId> _getCellIds() {
    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
    return grid.bbox2CellIds(this._bbox, true);
  }

  private List<Long> _getTimestamps() {
    return this._tstamps.getTimeStamps();
  }
  
  public <R, S> S mapReduce(Function<T, R> mapper, Supplier<S> identitySupplier, BiFunction<S, R, S> accumulator, BinaryOperator<S> combiner) throws Exception {
    if (this._forClass.equals(OSMContribution.class)) {
      return this.reduceCellsOSMContribution(this._getCellIds(), this._getTimestamps(), this._bbox, this._getPreFilter(), this._getFilter(), (Function<OSMContribution, R>) mapper, identitySupplier, accumulator, combiner);
    } else if (this._forClass.equals(OSMEntity.class)) {
      return this.reduceCellsOSMEntity(this._getCellIds(), this._getTimestamps(), this._bbox, this._getPreFilter(), this._getFilter(), (Function<OSMEntity, R>) mapper, identitySupplier, accumulator, combiner);
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      return this.reduceCellsOSMEntitySnapshot(this._getCellIds(), this._getTimestamps(), this._bbox, this._getPreFilter(), this._getFilter(), (Function<OSMEntitySnapshot, R>) mapper, identitySupplier, accumulator, combiner);
    } else throw new UnsupportedOperationException("No mapper implemented for your database type");
  }

  public <R extends Number> R sum(Function<T, R> f) throws Exception {
    return this.mapReduce(f, () -> (R) (Integer) 0, (x, y) -> NumberUtils.add(x, y), (x, y) -> NumberUtils.add(x, y));
  }

  // check if the `S identity` here also needs to be replaced with `Supplier<S> identitySupplier` to make it work for "complex" types of S
  public <R, S, U> SortedMap<U, S> mapAggregate(Function<T, Pair<U, R>> mapper, S identity, BiFunction<S, R, S> accumulator, BinaryOperator<S> combiner) throws Exception {
    return this.mapReduce(mapper, () -> new TreeMap(), (SortedMap<U, S> m, Pair<U, R> r) -> {
      m.put(r.getKey(), accumulator.apply(m.getOrDefault(r.getKey(), identity), r.getValue()));
      return m;
    }, (a,b) -> {
      SortedMap<U, S> combined = new TreeMap<>(a);
      for (SortedMap.Entry<U, S> entry: b.entrySet()) {
        combined.merge(entry.getKey(), entry.getValue(), combiner);
      }
      return combined;
    });
  }

  public <R extends Number, U> SortedMap<U, R> sumAggregate(Function<T, Pair<U, R>> mapper) throws Exception {
    return this.mapAggregate(mapper, (R) (Integer) 0, (x, y) -> NumberUtils.add(x, y), (x, y) -> NumberUtils.add(x, y));
  }

  public <R, S> SortedMap<Timestamp, S> mapAggregateByTimestamp(Function<T, R> mapper, S identity, BiFunction<S, R, S> accumulator, BinaryOperator<S> combiner) throws Exception {
    SortedMap<Timestamp, S> result;
    List<Timestamp> timestamps = this._getTimestamps().stream().map(Timestamp::new).collect(Collectors.toList());
    if (this._forClass.equals(OSMContribution.class)) {
      result = this.mapAggregate((Function<T, Pair<Timestamp, R>>) (t -> {
        int timeBinIndex = Collections.binarySearch(timestamps, ((OSMContribution) t).getTimestamp());
        if (timeBinIndex < 0) { timeBinIndex = -timeBinIndex - 2; }
        return new ImmutablePair<>(timestamps.get(timeBinIndex), mapper.apply(t));
      }), identity, accumulator, combiner);
      timestamps.remove(timestamps.size()-1); // pop last element from timestamps list, so it doesn't get nodata-filled with "0" below
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      result = this.mapAggregate((Function<T, Pair<Timestamp, R>>) (t ->
        new ImmutablePair<>(((OSMEntitySnapshot) t).getTimestamp(), mapper.apply(t))
      ), identity, accumulator, combiner);
    } else throw new UnsupportedOperationException("mapAggregateByTimestamp only allowed for OSMContribution and OSMEntitySnapshot");
    // fill nodata entries with "0"
    timestamps.forEach(ts -> result.putIfAbsent(ts, identity));
    return result;
  }

  public <R extends Number> SortedMap<Timestamp, R> sumAggregateByTimestamp(Function<T, R> mapper) throws Exception {
    return this.mapAggregateByTimestamp(mapper, (R) (Integer) 0, (x, y) -> NumberUtils.add(x, y), (x, y) -> NumberUtils.add(x, y));
  }
}
