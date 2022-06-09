package org.heigit.ohsome.oshdb.api.mapreducer.improve;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public abstract class MapReducerImprove<X> implements MapReducer<X>{
  protected final SerializableFunction<OSHEntity, Stream<X>> transform;


  public MapReducerImprove(SerializableFunction<OSHEntity, Stream<X>> transform) {
    this.transform = transform;
  }

  protected abstract <R> MapReducer<R> of(SerializableFunction<OSHEntity, Stream<R>> fnt);

  @Override
  public <R> MapReducer<R> map(SerializableFunction<X, R> map) {
    var fnt = transform.andThen(x -> x.map(map));
    return of(fnt);
  }

  @Override
  public <R> MapReducer<R> flatMap(SerializableFunction<X, Stream<R>> map) {
    var fnt = transform.andThen(x -> x.flatMap(map));
    return of(fnt);
  }

  public <R> MapReducer<R> flatMap(SerializableBiFunction<OSHEntity, Stream<X>, Stream<R>> map){
    SerializableFunction<OSHEntity, Stream<R>> fnt = osh -> map.apply(osh, transform.apply(osh));
    return of(fnt);
  }

  @Override
  public MapReducer<X> filter(SerializablePredicate<X> f) {
    var fnt = transform.andThen(x -> x.filter(f));
    return of(fnt);
  }

  @Override
  public <U extends Comparable<U> & Serializable> MapAggregator<U, X> aggregateBy(
      SerializableFunction<X, U> indexer, Collection<U> zerofill) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp()
      throws UnsupportedOperationException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal> MapAggregator<U, X> aggregateByGeometry(
      Map<U, P> geometries) {
    // TODO Auto-generated method stub
    return null;
  }
}
