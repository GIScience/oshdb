package org.heigit.ohsome.oshdb.api.mapreducer.base;

import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;

public class MapAggregatorOSHEntity<U, X> extends MapAggregatorBase<OSHEntity, U, X> {

  @Override
  public <R> MapAggregatorOSHEntity<U, R> map(SerializableFunction<X, R> map) {
    return null;
  }

  @Override
  public <R> MapAggregatorOSHEntity<U, R> flatMap(SerializableFunction<X, Stream<R>> map) {
    return null;
  }

  @Override
  public <V> MapAggregatorOSHEntity<CombinedIndex<U, V>, X> aggregateBy(
      SerializableFunction<X, V> indexer) {
    return null;
  }

}
