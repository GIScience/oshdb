package org.heigit.bigspatialdata.oshdb.api.mapper;

import com.vividsolutions.jts.geom.Geometry;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.generic.TriFunction;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.api.objects.Timestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.Timestamps;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

public abstract class Mapper {
  protected OSHDB _oshdb;
  private BoundingBox _bbox = null;
  private Timestamps _tstamps = null;
  private final List<Predicate<OSMEntity>> _filters = new ArrayList<>();
  protected TagInterpreter _tagInterpreter = null;
  
  protected Mapper(OSHDB oshdb) {
    this._oshdb = oshdb;
  }
  
  public static Mapper using(OSHDB oshdb) {
    if (oshdb instanceof OSHDB_H2) {
      Mapper mapper = new Mapper_H2((OSHDB_H2) oshdb);
      mapper._oshdb = oshdb;
      return mapper;
    } else throw new UnsupportedOperationException("No mapper implemented for your database type");
  }
  
  protected abstract <R, S> S reduceCells(Iterable<CellId> cellIds, List<Long> tstampsIds, BoundingBox bbox, Predicate<OSMEntity> filter, TriFunction<Timestamp, Geometry, OSMEntity, R> f, S s, BiFunction<S, R, S> rf) throws Exception;
  
  public Mapper boundingBox(BoundingBox bbox) {
    this._bbox = bbox;
    return this;
  }
  
  public Mapper timestamps(Timestamps tstamps) {
    this._tstamps = tstamps;
    return this;
  }
  
  public Mapper tagInterpreter(TagInterpreter tagInterpreter) {
    this._tagInterpreter = tagInterpreter;
    return this;
  }
  
  public Mapper filter(Predicate<OSMEntity> f) {
    this._filters.add(f);
    return this;
  }

  public <R> List<R> map(TriFunction<Timestamp, Geometry, OSMEntity, R> f) throws Exception {
    return this.reduceCells(this._getCellIds(), this._getTimestamps(), this._bbox, this._getFilter(), f, new ArrayList<>(), (l, r) -> {
      l.add(r);
      return l;
    });
  }

  public <R> List<R> flatMap(TriFunction<Timestamp, Geometry, OSMEntity, List<R>> f) throws Exception {
    return this.reduceCells(this._getCellIds(), this._getTimestamps(), this._bbox, this._getFilter(), f, new ArrayList<>(), (l, r) -> {
      l.addAll(r);
      return l;
    });
  }

  public Double sum(TriFunction<Timestamp, Geometry, OSMEntity, Double> f) throws Exception {
    return this.reduceCells(this._getCellIds(), this._getTimestamps(), this._bbox, this._getFilter(), f, 0., (s, r) -> s + r);
  }

  public SortedMap<Timestamp, Double> aggregate(TriFunction<Timestamp, Geometry, OSMEntity, Pair<Timestamp, Double>> f) throws Exception {
    return this.reduceCells(this._getCellIds(), this._getTimestamps(), this._bbox, this._getFilter(), f, new TreeMap<>(), (m, r) -> {
      m.put(r.getKey(), m.getOrDefault(r.getKey(), 0.) + r.getValue());
      return m;
    });
  }

  private Predicate<OSMEntity> _getFilter() {
    return this._filters.stream().reduce(Predicate::and).get();
  }

  private Iterable<CellId> _getCellIds() {
    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
    return grid.bbox2CellIds(this._bbox, true);
  }

  private List<Long> _getTimestamps() {
    return this._tstamps.getTimeStampIds();
  }
}
