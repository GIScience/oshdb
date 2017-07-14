package org.heigit.bigspatialdata.oshdb.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.generic.NumberUtils;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.api.objects.Timestamp;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.api.objects.Timestamps;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

public abstract class Mapper<T> {
  protected OSHDB _oshdb;
  protected OSHDB _oshdbForTags;
  protected Class _forClass = null;
  private BoundingBox _bbox = null;
  private Timestamps _tstamps = null;
  private final List<Predicate<OSHEntity>> _preFilters = new ArrayList<>();
  private final List<Predicate<OSMEntity>> _filters = new ArrayList<>();
  protected TagInterpreter _tagInterpreter = null;
  
  protected Mapper(OSHDB oshdb) {
    this._oshdb = oshdb;
  }
  
  public static <T> Mapper<T> using(OSHDB oshdb) {
    if (oshdb instanceof OSHDB_H2) {
      Mapper<T> mapper = new Mapper_H2((OSHDB_H2) oshdb);
      mapper._oshdb = oshdb;
      mapper._oshdbForTags = oshdb;
      return mapper;
    } else throw new UnsupportedOperationException("No mapper implemented for your database type");
  }
  
  public Mapper<T> usingForTags(OSHDB oshdb) {
    this._oshdbForTags = oshdb;
    return this;
  }
  
  protected abstract Integer getTagKeyId(String key) throws Exception;
  protected abstract Pair<Integer, Integer> getTagValueId(String key, String value) throws Exception;

  protected <R, S> S reduceCellsOSMContribution(Iterable<CellId> cellIds, List<Long> tstampsIds, BoundingBox bbox, Predicate<OSHEntity> preFilter, Predicate<OSMEntity> filter, Function<OSMContribution, R> f, S s, BiFunction<S, R, S> rf) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }
  
  protected <R, S> S reduceCellsOSMEntity(Iterable<CellId> cellIds, List<Long> tstampsIds, BoundingBox bbox, Predicate<OSHEntity> preFilter, Predicate<OSMEntity> filter, Function<OSMEntity, R> f, S s, BiFunction<S, R, S> rf) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }
  
  protected <R, S> S reduceCellsOSMEntitySnapshot(Iterable<CellId> cellIds, List<Long> tstampsIds, BoundingBox bbox, Predicate<OSHEntity> preFilter, Predicate<OSMEntity> filter, Function<OSMEntitySnapshot, R> f, S s, BiFunction<S, R, S> rf) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }
  
  public Mapper<T> boundingBox(BoundingBox bbox) {
    this._bbox = bbox;
    return this;
  }
  
  public Mapper<T> timestamps(Timestamps tstamps) {
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
  
  public <R> List<R> map(Function<T, R> f) throws Exception {
    return this.reduce(f, new ArrayList(), (l, r) -> {
      l.add(r);
      return l;
    });
  }
  
  public <R> List<R> flatmap(Function<T, Collection<R>> f) throws Exception {
    return this.reduce(f, new ArrayList(), (l, r) -> {
      l.addAll(r);
      return l;
    });
  }
  
  public <R extends Number> R sum(Function<T, R> f) throws Exception {
    return this.reduce(f, (R) (Integer) 0, (x, y) -> NumberUtils.add(x, y));
  }
  
  public <R, S> S reduce(Function<T, R> f, S s, BiFunction<S, R, S> rf) throws Exception {
    if (this._forClass.equals(OSMContribution.class)) {
      return this.reduceCellsOSMContribution(this._getCellIds(), this._getTimestamps(), this._bbox, this._getPreFilter(), this._getFilter(), (Function<OSMContribution, R>) f, s, rf);
    } else if (this._forClass.equals(OSMEntity.class)) {
      return this.reduceCellsOSMEntity(this._getCellIds(), this._getTimestamps(), this._bbox, this._getPreFilter(), this._getFilter(), (Function<OSMEntity, R>) f, s, rf);
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      return this.reduceCellsOSMEntitySnapshot(this._getCellIds(), this._getTimestamps(), this._bbox, this._getPreFilter(), this._getFilter(), (Function<OSMEntitySnapshot, R>) f, s, rf);
    } else throw new UnsupportedOperationException("No mapper implemented for your database type");
  }
  
  public <R, S> SortedMap<Timestamp, S> mapAggregateByTimestamp(Function<T, R> f, S s, BiFunction<S, R, S> rf) throws Exception {
    if (this._forClass.equals(OSMContribution.class)) {
      return this.mapAggregate((Function<T, Pair<Timestamp, R>>) (t -> new ImmutablePair<>(((OSMContribution) t).getTimestamp(), f.apply(t))), s, rf);
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      return this.mapAggregate((Function<T, Pair<Timestamp, R>>) (t -> new ImmutablePair<>(((OSMEntitySnapshot) t).getTimestamp(), f.apply(t))), s, rf);
    } else throw new UnsupportedOperationException("mapAggregateByTimestamp only allowed for OSMContribution and OSMEntitySnapshot");
  }
  
  public <R extends Number> SortedMap<Timestamp, R> sumAggregateByTimestamp(Function<T, R> f) throws Exception {
    return this.mapAggregateByTimestamp(f, (R) (Integer) 0, (x, y) -> NumberUtils.add(x, y));
  }
  
  public <R, S, U> SortedMap<U, S> mapAggregate(Function<T, Pair<U, R>> f, S s, BiFunction<S, R, S> rf) throws Exception {
    return this.reduce(f, new TreeMap(), (TreeMap<U, S> m, Pair<U, R> r) -> {
      m.put(r.getKey(), rf.apply(m.getOrDefault(r.getKey(), s), r.getValue()));
      return m;
    });
  }
  
  public <R extends Number, U> SortedMap<U, R> sumAggregate(Function<T, Pair<U, R>> f) throws Exception {
    return this.mapAggregate(f, (R) (Integer) 0, (x, y) -> NumberUtils.add(x, y));
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
    return this._tstamps.getTimeStampIds();
  }
}
