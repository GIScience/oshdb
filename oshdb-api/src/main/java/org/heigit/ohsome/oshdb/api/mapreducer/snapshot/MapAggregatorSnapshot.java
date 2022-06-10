package org.heigit.ohsome.oshdb.api.mapreducer.snapshot;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.base.GeometrySplitter;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorOSHDBMapReducible;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class MapAggregatorSnapshot<U extends Comparable<U> & Serializable, X>
    extends MapAggregatorOSHDBMapReducible<OSMEntitySnapshot, U, X> {

  MapAggregatorSnapshot(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<OSMEntitySnapshot>> toReducible,
      SerializableFunction<OSMEntitySnapshot, Stream<Map.Entry<U, X>>> transform) {
    super(oshdb, view, toReducible, transform);
  }

  protected <V  extends Comparable<V> & Serializable, R> MapAggregatorSnapshot<V, R> with(
      SerializableFunction<OSMEntitySnapshot, Stream<Map.Entry<V, R>>> transform) {
    return new MapAggregatorSnapshot<>(oshdb, view, toReducible, transform);
  }

  @Override
  public MapAggregatorSnapshot<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
    return with(snap -> Stream.of(snap.getTimestamp())
          .flatMap(ts -> transform.apply(snap)
              .map(ux -> combined(ux.getKey(), ts, ux.getValue()))));
  }

  @Override
  public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal>
      MapAggregatorSnapshot<OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(
          Map<V, P> geometries) {
    var gs = new GeometrySplitter<>(geometries);
    return with(snap -> gs.splitOSMEntitySnapshot(snap)
        .flatMap(vs -> transform.apply(vs.getValue())
            .map(ux -> combined(ux.getKey(), vs.getKey(), ux.getValue()))));
  }
}
