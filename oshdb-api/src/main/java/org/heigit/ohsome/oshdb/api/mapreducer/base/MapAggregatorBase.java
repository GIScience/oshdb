package org.heigit.ohsome.oshdb.api.mapreducer.base;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;

public abstract class MapAggregatorBase<B, U, X> {

  public abstract <R> MapAggregatorBase<B, U, R> map(SerializableFunction<X, R> map);

  public abstract <R> MapAggregatorBase<B, U, R> flatMap(SerializableFunction<X, Stream<R>> map);

  public abstract <V> MapAggregatorBase<B, CombinedIndex<U, V>, X> aggregateBy(SerializableFunction<X, V> indexer);


  protected <R> SerializableFunction<Entry<U, X>, Entry<U,R>> entryMap(SerializableFunction<X, R> map) {
    return ux -> Map.entry(ux.getKey(), map.apply(ux.getValue()));
  }

  protected <R> SerializableFunction<Entry<U, X>, Stream<Entry<U, R>>> entryFlatMap(SerializableFunction<X, Stream<R>> map){
    return ux -> map.apply(ux.getValue()).map(r -> Map.entry(ux.getKey(), r));
  }
}
