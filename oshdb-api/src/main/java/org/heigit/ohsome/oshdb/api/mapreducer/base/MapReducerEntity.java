package org.heigit.ohsome.oshdb.api.mapreducer.base;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class MapReducerEntity<X> extends MapReducerBase2<OSHEntity, X> {

  public MapReducerEntity(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<X>> transform) {
    super(oshdb, view, transform);
  }

  @Override
  protected <R> MapReducerEntity<R> with(
      SerializableFunction<OSHEntity, Stream<R>> transform) {
    return new MapReducerEntity<>(oshdb, view, transform);
  }

  @Override
  protected <U extends Comparable<U> & Serializable, R>
      MapAggregatorBase2<OSHEntity, U, R> aggregator(
          SerializableFunction<OSHEntity, Stream<Entry<U, R>>> transform) {
    return new MapAggregatorEntity<>(oshdb, view, transform);
  }

  @Override
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp()
      throws UnsupportedOperationException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <U extends Comparable<U> & Serializable,
          P extends Geometry & Polygonal> MapAggregator<U, X> aggregateByGeometry(
      Map<U, P> geometries) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected SerializableFunction<OSHEntity, Stream<X>> getTransformer() {
    return transform;
  }
}
