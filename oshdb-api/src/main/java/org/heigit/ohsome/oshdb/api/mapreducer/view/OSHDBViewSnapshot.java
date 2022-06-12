package org.heigit.ohsome.oshdb.api.mapreducer.view;

import static java.util.Map.entry;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.base.GeometrySplitter;
import org.heigit.ohsome.oshdb.api.mapreducer.base3.MapAggregatorBase3;
import org.heigit.ohsome.oshdb.api.mapreducer.base3.MapReducerBase3;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshotImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class OSHDBViewSnapshot {

  public MapReducer<OSMEntitySnapshot> view() {
    OSHDBDatabase oshdb = null;
    OSHDBView<?> view = null;
    var cellIterator = view.getCellIterator();

    return new MR<>(oshdb, view,
        osh -> cellIterator.iterateByTimestamps(osh, false)
          .map(OSMEntitySnapshotImpl::new)
          .map(OSMEntitySnapshot.class::cast),
        Stream::of);
  }

  private static class MR<X> extends MapReducerBase3<OSMEntitySnapshot, X> {

    protected MR(OSHDBDatabase oshdb, OSHDBView<?> view,
        SerializableFunction<OSHEntity, Stream<OSMEntitySnapshot>> toReducible,
        SerializableFunction<OSMEntitySnapshot, Stream<X>> transform) {
      super(oshdb, view, toReducible, transform);
    }

    @Override
    protected <R> MapReducerBase3<OSMEntitySnapshot, R> with(
        SerializableFunction<OSMEntitySnapshot, Stream<R>> transform) {
      return new MR<>(oshdb, view, toReducible, transform);
    }

    @Override
    protected <U extends Comparable<U> & Serializable, R>
        MapAggregatorBase3<OSMEntitySnapshot, U, R> aggregator(
        SerializableFunction<OSMEntitySnapshot, Stream<Entry<U, R>>> transform) {
      return new MA<>(oshdb, view, toReducible, transform);
    }

    @Override
    public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp()
        throws UnsupportedOperationException {
      return aggregator(snap -> Stream.of(entry(snap.getTimestamp(), snap))
          .flatMap(ts -> transform.apply(ts.getValue())
              .map(x -> entry(ts.getKey(), x))));
    }

    @Override
    public <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal>
        MapAggregator<U, X> aggregateByGeometry(Map<U, P> geometries) {
      var gs = new GeometrySplitter<>(geometries);
      return aggregator(snap -> gs.splitOSMEntitySnapshot(snap)
          .flatMap(us -> transform.apply(us.getValue())
              .map(x -> entry(us.getKey(), x))));
    }
  }

  private static class MA<U extends Comparable<U> & Serializable, X>
      extends MapAggregatorBase3<OSMEntitySnapshot, U, X> {

    protected MA(OSHDBDatabase oshdb, OSHDBView<?> view,
        SerializableFunction<OSHEntity, Stream<OSMEntitySnapshot>> toReducible,
        SerializableFunction<OSMEntitySnapshot, Stream<Entry<U, X>>> transform) {
      super(oshdb, view, toReducible, transform);
    }

    @Override
    protected <V extends Comparable<V> & Serializable, R>
        MapAggregatorBase3<OSMEntitySnapshot, V, R> with(
        SerializableFunction<OSMEntitySnapshot, Stream<Entry<V, R>>> transform) {
      return new MA<>(oshdb, view, toReducible, transform);
    }

    @Override
    public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
      return with(snap -> Stream.of(snap.getTimestamp())
          .flatMap(ts -> transform.apply(snap)
              .map(ux -> combine(ux, ts))));
    }

    @Override
    public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal>
        MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(Map<V, P> geometries) {
      var gs = new GeometrySplitter<>(geometries);
      return with(contrib -> gs.split(contrib)
          .flatMap(vs -> transform.apply(vs.getValue())
              .map(ux -> combine(ux, vs.getKey()))));
    }
  }
}
