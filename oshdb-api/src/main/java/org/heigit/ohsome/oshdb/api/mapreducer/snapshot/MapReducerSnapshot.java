package org.heigit.ohsome.oshdb.api.mapreducer.snapshot;

import static java.util.Map.entry;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.base.GeometrySplitter;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerOSHDBMapReducible;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class MapReducerSnapshot<X> extends MapReducerOSHDBMapReducible<OSMEntitySnapshot, X> {

  MapReducerSnapshot(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<OSMEntitySnapshot>> toReducible,
      SerializableFunction<OSMEntitySnapshot, Stream<X>> transform) {
    super(oshdb, view, toReducible, transform);
  }

  @Override
  protected <R> MapReducerSnapshot<R> with(
      SerializableFunction<OSMEntitySnapshot, Stream<R>> transform) {
    return new MapReducerSnapshot<>(oshdb, view, toReducible, transform);
  }

  @Override
  protected <U extends Comparable<U> & Serializable, R> MapAggregatorSnapshot<U, R> aggregator(
      SerializableFunction<OSMEntitySnapshot, Stream<Entry<U, R>>> transform) {
    return new MapAggregatorSnapshot<>(oshdb, view, toReducible, transform);
  }

  @Override
  public MapAggregatorSnapshot<OSHDBTimestamp, X> aggregateByTimestamp() {
    return aggregator(snap -> Stream.of(snap.getTimestamp())
        .flatMap(ts -> transform.apply(snap)
            .map(x -> entry(ts, x))));
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
