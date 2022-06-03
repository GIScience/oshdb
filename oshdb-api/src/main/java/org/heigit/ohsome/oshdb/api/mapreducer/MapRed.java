package org.heigit.ohsome.oshdb.api.mapreducer;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.io.Serializable;
import org.heigit.ohsome.oshdb.api.generic.NumberUtils;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;

/**
 *
 *
 */
public interface MapRed<X> {

  MapRed<X> filter(String f);

  MapRed<X> filter(FilterExpression f);

  // Mappable
  default MapRed<X> filter(SerializablePredicate<X> f) {
    return flatMap(data -> f.test(data) ? singletonList(data) : emptyList());
  }

  <R> MapRed<R> map(SerializableFunction<X, R> mapper);

  <R> MapRed<R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper);


  // MapAggregateable
  <U extends Serializable> MapAgg<U, X> aggregateBy(SerializableFunction<X, U> indexer);


  // MapReducerAggregations
  <S> S reduce(
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator,
      SerializableBinaryOperator<S> combiner)
      throws Exception;

  default X reduce(SerializableSupplier<X> identitySupplier,
      SerializableBinaryOperator<X> accumulator) throws Exception {
    return reduce(identitySupplier, accumulator::apply, accumulator);
  }

  default <R extends Number> R sum(SerializableFunction<X, R> mapper) throws Exception {
    return map(mapper).reduce(() -> (R) (Integer) 0, NumberUtils::add);
  }

  default Integer count() throws Exception {
    return this.sum(ignored -> 1);
  }
}
