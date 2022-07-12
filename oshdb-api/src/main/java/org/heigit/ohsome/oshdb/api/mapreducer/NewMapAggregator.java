package org.heigit.ohsome.oshdb.api.mapreducer;

import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;

public interface NewMapAggregator<U, X> {

  <R> NewMapAggregator<U, R> map(SerializableFunction<X, R> map);

  <R> NewMapAggregator<U, R> flatMap(SerializableFunction<X, Stream<R>> map);

  <V> NewMapAggregator<CombinedIndex<U, V>, X> aggregateBy(SerializableFunction<X, V> indexer);

}
