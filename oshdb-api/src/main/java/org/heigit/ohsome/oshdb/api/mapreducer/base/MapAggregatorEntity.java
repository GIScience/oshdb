package org.heigit.ohsome.oshdb.api.mapreducer.base;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class MapAggregatorEntity<U extends Comparable<U> & Serializable, X>
    extends MapAggregatorBase2<OSHEntity, U, X> {

  protected MapAggregatorEntity(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<Entry<U, X>>> transform) {
    super(oshdb, view, transform);
  }

  @Override
  protected <V  extends Comparable<V> & Serializable, R> MapAggregatorEntity<V, R> with(
      SerializableFunction<OSHEntity, Stream<Entry<V, R>>> transform) {
    return new MapAggregatorEntity<>(oshdb, view, transform);
  }

  @Override
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V extends Comparable<V> & Serializable,
          P extends Geometry & Polygonal> MapAggregator<OSHDBCombinedIndex<U, V>, X>
      aggregateByGeometry(Map<V, P> geometries) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected SerializableFunction<OSHEntity, Stream<Entry<U, X>>> getTransformer() {
    // TODO Auto-generated method stub
    return null;
  }

}
