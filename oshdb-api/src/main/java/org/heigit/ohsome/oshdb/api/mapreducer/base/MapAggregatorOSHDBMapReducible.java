package org.heigit.ohsome.oshdb.api.mapreducer.base;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public abstract class MapAggregatorOSHDBMapReducible<T extends OSHDBMapReducible,
    U extends Comparable<U> & Serializable, X> extends MapAggregatorBase2<T, U, X> {

  protected final SerializableFunction<OSHEntity, Stream<T>> toReducible;

  protected MapAggregatorOSHDBMapReducible(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<T>> toReducible,
      SerializableFunction<T, Stream<Entry<U, X>>> transform) {
    super(oshdb, view, transform);
    this.toReducible = toReducible;
  }

  @Override
  public abstract MapAggregatorOSHDBMapReducible<T, OSHDBCombinedIndex<U, OSHDBTimestamp>, X>
      aggregateByTimestamp();

  @Override
  public abstract <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal>
      MapAggregatorOSHDBMapReducible<T, OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(
          Map<V, P> geometries);

  @Override
  protected SerializableFunction<OSHEntity, Stream<Entry<U, X>>> getTransformer() {
    return osh -> toReducible.apply(osh).flatMap(transform);
  }


}
