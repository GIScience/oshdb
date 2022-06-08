package org.heigit.ohsome.oshdb.api.mapreducer.improved;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public abstract class MapReducerImproved<X> implements MapReducer<X>{
  protected SerializableFunction<OSHEntity, Stream<X>> transform;

  protected MapReducerImproved(SerializableFunction<OSHEntity, Stream<X>> fnt) {
    this.transform = fnt;
  }

  protected abstract <Y> MapReducerImproved<Y> instance(
      SerializableFunction<OSHEntity, Stream<Y>> fnt);

  @Override
  public <R> MapReducer<R> map(SerializableFunction<X, R> map) {
    var fnt = transform.andThen(s -> s.map(map));
    return instance(fnt);
  }

  @Override
  public <R> MapReducer<R> flatMap(SerializableFunction<X, Stream<R>> map) {
    var fnt = transform.andThen(s -> s.flatMap(map));
    return instance(fnt);
  }

  @Override
  public MapReducer<X> filter(SerializablePredicate<X> f) {
    var fnt = transform.andThen(s -> s.filter(f));
    return instance(fnt);
  }

  @Override
  public <U extends Comparable<U> & Serializable> MapAggregator<U, X> aggregateBy(
      SerializableFunction<X, U> indexer, Collection<U> zerofill) {
    return null;
  }

  @Override
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp()
      throws UnsupportedOperationException {
    return null;
  }

  @Override
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    return null;
  }

  @Override
  public <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal> MapAggregator<U, X> aggregateByGeometry(
      Map<U, P> geometries) {
    return null;
  }
}
