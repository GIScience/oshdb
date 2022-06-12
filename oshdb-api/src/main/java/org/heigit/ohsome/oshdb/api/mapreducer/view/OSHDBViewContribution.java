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
import org.heigit.ohsome.oshdb.api.object.OSMContributionImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class OSHDBViewContribution {

  public MapReducer<OSMContribution> view() {
    OSHDBDatabase oshdb = null;
    OSHDBView<?> view = null;
    var cellIterator = view.getCellIterator();

    return new MR<>(oshdb, view,
        osh -> cellIterator.iterateByContribution(osh, false)
          .map(OSMContributionImpl::new)
          .map(OSMContribution.class::cast),
        Stream::of);
  }

  private static class MR<X> extends MapReducerBase3<OSMContribution, X> {

    protected MR(OSHDBDatabase oshdb, OSHDBView<?> view,
        SerializableFunction<OSHEntity, Stream<OSMContribution>> toReducible,
        SerializableFunction<OSMContribution, Stream<X>> transform) {
      super(oshdb, view, toReducible, transform);
    }

    @Override
    protected <R> MapReducerBase3<OSMContribution, R> with(
        SerializableFunction<OSMContribution, Stream<R>> transform) {
      return new MR<>(oshdb, view, toReducible, transform);
    }

    @Override
    protected <U extends Comparable<U> & Serializable, R>
        MapAggregatorBase3<OSMContribution, U, R> aggregator(
        SerializableFunction<OSMContribution, Stream<Entry<U, R>>> transform) {
      return new MA<>(oshdb, view, toReducible, transform);
    }

    @Override
    public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp()
        throws UnsupportedOperationException {
      var timestamps = view.getTimestamps().get();
      return aggregator(contrib -> transform.apply(contrib)
          .map(x -> entry(timestamps.floor(contrib.getTimestamp()), x)));
    }

    @Override
    public <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal>
        MapAggregator<U, X> aggregateByGeometry(Map<U, P> geometries) {
      var gs = new GeometrySplitter<>(geometries);
      return aggregator(snap -> gs.splitOSMContribution(snap)
          .flatMap(us -> transform.apply(us.getValue())
              .map(x -> entry(us.getKey(), x))));
    }
  }

  private static class MA<U extends Comparable<U> & Serializable, X>
      extends MapAggregatorBase3<OSMContribution, U, X> {

    protected MA(OSHDBDatabase oshdb, OSHDBView<?> view,
        SerializableFunction<OSHEntity, Stream<OSMContribution>> toReducible,
        SerializableFunction<OSMContribution, Stream<Entry<U, X>>> transform) {
      super(oshdb, view, toReducible, transform);
    }

    @Override
    protected <V extends Comparable<V> & Serializable, R>
        MapAggregatorBase3<OSMContribution, V, R> with(
        SerializableFunction<OSMContribution, Stream<Entry<V, R>>> transform) {
      return new MA<>(oshdb, view, toReducible, transform);
    }

    @Override
    public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
      var timestamps = view.getTimestamps().get();
      return with(contrib -> Stream.of(timestamps.floor(contrib.getTimestamp()))
          .flatMap(ts -> transform.apply(contrib)
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
