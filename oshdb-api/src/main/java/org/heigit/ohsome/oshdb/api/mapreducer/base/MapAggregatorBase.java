package org.heigit.ohsome.oshdb.api.mapreducer.base;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;

public abstract class MapAggregatorBase<B, U, X> {
  protected MapReducerBase<B, Entry<U, X>> mr;

  protected MapAggregatorBase(MapReducerBase<B, Entry<U, X>> mr) {
    this.mr = mr;
  }

  public abstract <R> MapAggregatorBase<B, U, R> map(SerializableFunction<X, R> map);

  public abstract <R> MapAggregatorBase<B, U, R> flatMap(SerializableFunction<X, Stream<R>> map);

  public abstract <R> MapAggregatorBase<B, U, R> flatMapIterable(SerializableFunction<X, Iterable<R>> map);

  public abstract MapAggregatorBase<B, U, X> filter(SerializablePredicate<X> predicate);

  public abstract <V> MapAggregatorBase<B, CombinedIndex<U, V>, X> aggregateBy(SerializableFunction<X, V> indexer);

  public <S> Map<U, S> reduce(SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
    return mr.reduce(HashMap::new,
        (m, r) -> {
          var s = accumulator.apply(
              m.getOrDefault(r.getKey(), identitySupplier.get()), r.getValue());
          m.put(r.getKey(), s);
          return m; },
        (a, b) -> {
          var combined = new HashMap<>(a);
          b.entrySet().forEach(entry -> combined.merge(entry.getKey(), entry.getValue(), combiner));
          return combined; });
  }

  public Map<U, X> reduce(SerializableSupplier<X> identity, SerializableBinaryOperator<X> accumulator) {
    return this.reduce(identity, accumulator::apply, accumulator);
  }

  public <S> Map<U, S>  reduce(Function<MapAggregatorBase<B, U, X>, Map<U, S>> reducer){
    return reducer.apply(this);
  }

  public Stream<Entry<U, X>> stream() {
    return mr.stream();
  }

  public void forEach(BiConsumer<U, X> consumer) {
    stream().forEach(entry -> consumer.accept(entry.getKey(), entry.getValue()));
  }

  public Map<U, Long> count() {
    return map(x -> 1).reduce(Agg::sumInt);
  }


  protected <R> SerializableFunction<Entry<U, X>, Entry<U,R>> entryMap(SerializableFunction<X, R> map) {
    return ux -> Map.entry(ux.getKey(), map.apply(ux.getValue()));
  }

  protected <R> SerializableFunction<Entry<U, X>, Stream<Entry<U, R>>> entryFlatMap(SerializableFunction<X, Stream<R>> map){
    return ux -> map.apply(ux.getValue()).map(r -> Map.entry(ux.getKey(), r));
  }

  protected SerializablePredicate<Entry<U, X>> entryFilter(SerializablePredicate<X> predicate) {
    return ux -> predicate.test(ux.getValue());
  }
}
