package org.heigit.ohsome.oshdb.api.mapreducer.base3;

import static java.util.Map.entry;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.generic.OSHDBCombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public abstract class MapAggregatorBase3<T, U extends Comparable<U> & Serializable, X>
    implements MapAggregator<U, X> {
  protected final OSHDBDatabase oshdb;
  protected final OSHDBView<?> view;
  protected final SerializableFunction<OSHEntity, Stream<T>> toReducible;
  protected final SerializableFunction<T, Stream<Entry<U, X>>> transform;

  protected MapAggregatorBase3(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<T>> toReducible,
      SerializableFunction<T, Stream<Entry<U, X>>> transform) {
    this.oshdb = oshdb;
    this.view = view;
    this.toReducible = toReducible;
    this.transform = transform;
  }

  protected abstract <V extends Comparable<V> & Serializable, R> MapAggregatorBase3<T, V, R>
      with(SerializableFunction<T, Stream<Entry<V, R>>> transform);

  @Override
  public <R> MapAggregator<U, R> map(SerializableFunction<X, R> map) {
    return with(transform.andThen(s -> s.map(
        ux -> entry(ux.getKey(), map.apply(ux.getValue())))));
  }

  @Override
  public <R> MapAggregator<U, R> flatMap(SerializableFunction<X, Stream<R>> map) {
    return with(transform.andThen(s -> s.flatMap(
        ux -> map.apply(ux.getValue()).map(r -> entry(ux.getKey(), r)))));
  }

  @Override
  public MapAggregator<U, X> filter(SerializablePredicate<X> f) {
    return with(transform.andThen(s -> s.filter(
        ux -> f.test(ux.getValue()))));
  }

  @Override
  public <V extends Comparable<V> & Serializable>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateBy(
          SerializableFunction<X, V> indexer) {
    return with(transform.andThen(s -> s.map(
        ux -> combine(ux, indexer))));
  }

  @Override
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
    throw new UnsupportedOperationException("This view, don't support auto timestamp aggregation");
  }

  @Override
  public MapAggregator<OSHDBCombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    return aggregateBy(view.getTimestamps().indexTimestamp(indexer));
  }

  @Override
  public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal>
      MapAggregator<OSHDBCombinedIndex<U, V>, X> aggregateByGeometry(
          Map<V, P> geometries) {
    throw new UnsupportedOperationException("This view, don't support auto geometry aggregation");
  }

  private SerializableFunction<OSHEntity, Stream<Entry<U, X>>> transformer() {
    return osh -> toReducible.apply(osh).flatMap(transform);
  }

  @Override
  public <S> SortedMap<U, S> reduce(SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
    var transformer = transformer();
    return oshdb.query(view,
        entities -> entities.flatMap(transformer)
          .reduce(hashMap(),
              acc(identitySupplier, accumulator),
              (a, b) -> merge(combiner, hashMap(), a, b)),
        sortedMap(),
        (a, b) -> merge(combiner, sortedMap(), a, b),
        (a, b) -> merge(combiner, sortedMap(), a, b));
  }

  @Override
  public Stream<Entry<U, X>> stream() {
    var transformer = transformer();
    return oshdb.query(view, entities -> entities.flatMap(transformer));
  }

  private static <U, X> Map<U, X> hashMap() {
    return new HashMap<>();
  }

  private static <U, X> SortedMap<U, X> sortedMap() {
    return new TreeMap<>();
  }

  private static <U, X, S> BiFunction<Map<U, S>, Entry<U, X>, Map<U, S>> acc(
      Supplier<S> identity, BiFunction<S, X, S> acc) {
    return (m, e) -> acc(identity, acc, m, e);
  }

  private static <U, X, S> Map<U, S> acc(
      Supplier<S> identity, BiFunction<S, X, S> acc, Map<U, S> map, Entry<U, X> entry) {
    var u = entry.getKey();
    var x = entry.getValue();
    map.compute(u, (k, v) -> acc.apply(v == null ? identity.get() : v, x));
    return map;
  }

  private static <U, X, M extends Map<U, X>> M merge(
      BinaryOperator<X> combiner, M merged, Map<U, X> a, Map<U, X> b) {
    merged.putAll(a);
    b.forEach((u, x) -> merged.merge(u, x, combiner));
    return merged;
  }

  protected <R> SerializableFunction<Map.Entry<U, X>, Map.Entry<U, R>>
      mapValue(SerializableFunction<X, R> map) {
    return ux -> entry(ux.getKey(), map.apply(ux.getValue()));
  }

  protected <V extends Comparable<V> & Serializable>
      Entry<OSHDBCombinedIndex<U, V>, X> combine(Entry<U, X> ux,
          SerializableFunction<X, V> indexer) {
    return combine(ux, indexer.apply(ux.getValue()));
  }

  protected <V extends Comparable<V> & Serializable>
      Entry<OSHDBCombinedIndex<U, V>, X> combine(Entry<U, X> ux,
          V v) {
    return entry(new OSHDBCombinedIndex<>(
        ux.getKey(), v), ux.getValue());
  }


}
