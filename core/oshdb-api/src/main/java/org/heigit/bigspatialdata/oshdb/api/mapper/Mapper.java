package org.heigit.bigspatialdata.oshdb.api.mapper;

import java.io.Serializable;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Ignite;
import org.heigit.bigspatialdata.oshdb.api.generic.NumberUtils;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSMType;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

public abstract class Mapper<T> {
  protected OSHDB _oshdb;
  protected OSHDB _oshdbForTags;
  protected Class _forClass = null;
  private BoundingBox _bbox = null;
  private OSHDBTimestamps _tstamps = null;
  private final List<SerializablePredicate<OSHEntity>> _preFilters = new ArrayList<>();
  private final List<SerializablePredicate<OSMEntity>> _filters = new ArrayList<>();
  protected TagInterpreter _tagInterpreter = null;
  protected EnumSet<OSMType> _typeFilter = EnumSet.allOf(OSMType.class);
  
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
    } else if (oshdb instanceof OSHDB_Ignite) {
      Mapper<T> mapper = new Mapper_Ignite<T>(oshdb);
      mapper._oshdbForTags = null;
      mapper._forClass = forClass;
      return mapper;
    } else {
      throw new UnsupportedOperationException("No mapper implemented for your database type");
    }
  }
  
  public Mapper<T> usingForTags(OSHDB oshdb) {
    this._oshdbForTags = oshdb;
    return this;
  }
  
  protected abstract Integer getTagKeyId(String key) throws Exception;
  protected abstract Pair<Integer, Integer> getTagValueId(String key, String value) throws Exception;

  public interface SerializableSupplier<R> extends Supplier<R>, Serializable {}
  public interface SerializablePredicate<T> extends Predicate<T>, Serializable {}
  public interface SerializableBinaryOperator<T> extends BinaryOperator<T>, Serializable {}
  public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}
  public interface SerializableBiFunction<T1, T2, R> extends BiFunction<T1, T2, R>, Serializable {}

  protected <R, S> S reduceCellsOSMContribution(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }
  
  protected <R, S> S reduceCellsOSMEntitySnapshot(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
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

  public Mapper<T> osmTypes(EnumSet<OSMType> typeFilter) {
    this._typeFilter = typeFilter;
    return this;
  }

  public Mapper<T> osmTypes(OSMType type1) {
    return this.osmTypes(EnumSet.of(type1));
  }
  public Mapper<T> osmTypes(OSMType type1, OSMType type2) {
    return this.osmTypes(EnumSet.of(type1, type2));
  }
  public Mapper<T> osmTypes(OSMType type1, OSMType type2, OSMType type3) {
    return this.osmTypes(EnumSet.of(type1, type2, type3));
  }
  
  public Mapper<T> tagInterpreter(TagInterpreter tagInterpreter) {
    this._tagInterpreter = tagInterpreter;
    return this;
  }
  
  public Mapper<T> filter(SerializablePredicate<OSMEntity> f) {
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
    return grid.bbox2CellIds(this._bbox, true);
  }

  private List<Long> _getTimestamps() {
    return this._tstamps.getTimestamps();
  }
  
  public <R, S> S mapReduce(SerializableFunction<T, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    if (this._forClass.equals(OSMContribution.class)) {
      return this.reduceCellsOSMContribution(this._getCellIds(), this._getTimestamps(), this._bbox, this._getPreFilter(), this._getFilter(), (SerializableFunction<OSMContribution, R>) mapper, identitySupplier, accumulator, combiner);
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      return this.reduceCellsOSMEntitySnapshot(this._getCellIds(), this._getTimestamps(), this._bbox, this._getPreFilter(), this._getFilter(), (SerializableFunction<OSMEntitySnapshot, R>) mapper, identitySupplier, accumulator, combiner);
    } else throw new UnsupportedOperationException("No mapper implemented for your database type");
  }

  public <R extends Number> R sum(SerializableFunction<T, R> f) throws Exception {
    return this.mapReduce(f, () -> (R) (Integer) 0, (SerializableBiFunction<R, R, R>)(x,y) -> NumberUtils.add(x,y), (SerializableBinaryOperator<R>)(x,y) -> NumberUtils.add(x,y));
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

  public <R extends Number, U> SortedMap<U, R> sumAggregate(SerializableFunction<T, Pair<U, R>> mapper) throws Exception {
    return this.mapAggregate(mapper, () -> (R) (Integer) 0, (SerializableBiFunction<R, R, R>)(x,y) -> NumberUtils.add(x,y), (x,y) -> NumberUtils.add(x,y));
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

  public <R extends Number> SortedMap<OSHDBTimestamp, R> sumAggregateByTimestamp(SerializableFunction<T, R> mapper) throws Exception {
    return this.mapAggregateByTimestamp(mapper, () -> (R) (Integer) 0, (SerializableBiFunction<R, R, R>)(x,y) -> NumberUtils.add(x,y), (x,y) -> NumberUtils.add(x,y));
  }
}
