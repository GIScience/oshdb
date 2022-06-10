package org.heigit.ohsome.oshdb.api.mapreducer.base;

import static java.util.Map.entry;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;

public abstract class MapAggregatorBase2<T, U extends Comparable<U> & Serializable, X>
    implements MapAggregator<U, X>  {

  protected final OSHDBDatabase oshdb;
  protected final OSHDBView<?> view;
  protected final SerializableFunction<T, Stream<Map.Entry<U, X>>> transform;

  protected MapAggregatorBase2(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<T, Stream<Entry<U, X>>> transform) {
    this.oshdb = oshdb;
    this.view = view;
    this.transform = transform;
  }

  protected abstract <V extends Comparable<V> & Serializable, R> MapAggregatorBase2<T, V, R> with(
      SerializableFunction<T, Stream<Map.Entry<V, R>>> transform);

  @Override
  public <R> MapAggregatorBase2<T, U, R> map(SerializableFunction<X, R> map) {
    return with(transform.andThen(s -> s.map(mapValue(map))));
  }

  @Override
  public <R> MapAggregator<U, R> flatMap(SerializableFunction<X, Stream<R>> map) {
    return with(transform
        .andThen(s -> s.flatMap(ux -> map.apply(ux.getValue()).map(r -> entry(ux.getKey(), r)))));
  }

  @Override
  public MapAggregator<U, X> filter(SerializablePredicate<X> f) {
    return with(transform.andThen(s -> s.filter(ux -> f.test(ux.getValue()))));
  }

  @Override
  public <V extends Comparable<V> & Serializable> MapAggregator<OSHDBCombinedIndex<U, V>, X>
      aggregateBy(SerializableFunction<X, V> indexer) {
    return with(transform.andThen(s -> s
        .map(ux -> entry(new OSHDBCombinedIndex<>(ux.getKey(), indexer.apply(ux.getValue())),
            ux.getValue()))));
  }

  @Override
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    indexTimestamp(view.getTimestamps().get(), indexer);
    return aggregateBy(indexTimestamp(view.getTimestamps().get(), indexer));
  }

  protected abstract SerializableFunction<OSHEntity, Stream<Map.Entry<U, X>>> getTransformer();

  @Override
  public <S> SortedMap<U, S> reduce(SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
    var transformer = getTransformer();
    return oshdb.query(view,
      entities -> entities.flatMap(transformer)
        .reduce((Map<U, S>) new HashMap<>(),
            (map, entry) -> {
              map.compute(entry.getKey(), (k, v) -> v == null
                  ? accumulator.apply(identitySupplier.get(), entry.getValue()) :
                    accumulator.apply(v, entry.getValue()));
              return map; },
            (a, b) -> {
              var map = new HashMap<U, S>();
              map.putAll(a);
              b.forEach((k, v) -> map.merge(k, v, combiner));
              return map; }),
        new TreeMap<U, S>(),
        (a, b) -> {
          var c = new TreeMap<U, S>();
          c.putAll(a);
          b.forEach((k, v) -> c.merge(k, v, combiner));
          return c;
        },
        (a, b) -> {
          var c = new TreeMap<U, S>();
          c.putAll(a);
          b.forEach((k, v) -> c.merge(k, v, combiner));
          return c;
        });
  }

  @Override
  public Stream<Entry<U, X>> stream() {
    var transformer = getTransformer();
    return oshdb.query(view, entities -> entities.flatMap(transformer));
  }

  protected static <A extends Comparable<A> & Serializable,
                   B extends Comparable<B> & Serializable, C>
      Map.Entry<OSHDBCombinedIndex<A, B>, C> combined(A u, B v, C x) {
    return Map.entry(new OSHDBCombinedIndex<>(u, v), x);
  }

  protected static <U, X, R> Function<Map.Entry<U, X>, Map.Entry<U, R>>
      mapValue(Function<X, R> map) {
    return ux -> Map.entry(ux.getKey(), map.apply(ux.getValue()));
  }

  protected static <U, X, R> Function<Map.Entry<U, X>, Stream<Map.Entry<U, R>>>
      flatMapValue(Function<X, Stream<R>> map) {
    return ux -> map.apply(ux.getValue()).map(r -> Map.entry(ux.getKey(), r));
  }

  protected static <X> SerializableFunction<X, OSHDBTimestamp>
      indexTimestamp(TreeSet<OSHDBTimestamp> timestamps,
        SerializableFunction<X, OSHDBTimestamp> indexer) {
    var minTime = timestamps.first();
    var maxTime = timestamps.last();
    return x -> {
      var ts = indexer.apply(x);
      if (ts == null || ts.compareTo(minTime) < 0 || ts.compareTo(maxTime) > 0) {
        throw new OSHDBInvalidTimestampException(
            "Aggregation timestamp outside of time query interval.");
      }
      return timestamps.floor(ts);
    };
  }

}
