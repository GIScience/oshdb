package org.heigit.ohsome.oshdb.api.mapreducer.contribution;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.base.GeometrySplitter;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorOSHDBMapReducible;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

class MapAggregatorContribution<U extends Comparable<U> & Serializable, X>
    extends MapAggregatorOSHDBMapReducible<OSMContribution, U, X> {

  protected MapAggregatorContribution(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<OSMContribution>> toReducible,
      SerializableFunction<OSMContribution, Stream<Entry<U, X>>> transform) {
    super(oshdb, view, toReducible, transform);
  }

  @Override
  protected <V extends Comparable<V> & Serializable, R>
      MapAggregatorContribution<V, R> with(
          SerializableFunction<OSMContribution, Stream<Entry<V, R>>> transform) {
    return new MapAggregatorContribution<>(oshdb, view, toReducible, transform);
  }

  @Override
  public MapAggregatorContribution<OSHDBCombinedIndex<U, OSHDBTimestamp>, X>
      aggregateByTimestamp() {
    var timestamps = view.getTimestamps().get();
    return with(contrib -> Stream.of(timestamps.floor(contrib.getTimestamp()))
        .flatMap(ts -> transform.apply(contrib)
            .map(ux -> combined(ux.getKey(), ts, ux.getValue()))));
  }

  @Override
  public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal>
      MapAggregatorContribution<OSHDBCombinedIndex<U, V>, X>
        aggregateByGeometry(Map<V, P> geometries) {
    var gs = new GeometrySplitter<>(geometries);
    return with(contrib -> gs.splitOSMContribution(contrib)
        .flatMap(vs -> transform.apply(vs.getValue())
            .map(ux -> combined(ux.getKey(), vs.getKey(), ux.getValue()))));
  }
}
