package org.heigit.ohsome.oshdb.api.mapreducer.base;

import static java.util.Map.entry;
import static java.util.Optional.ofNullable;

import com.google.common.collect.Streams;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Collector;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Reduce;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;

public class MapAggregatorBase<U, X> {
  protected MapReducerBase<Entry<U, X>> mr;

  public MapAggregatorBase(MapReducerBase<Entry<U, X>> mr) {
    this.mr = mr;
  }

  private <V, R> MapAggregatorBase<V, R> with(MapReducerBase<Entry<V, R>> mr) {
    return new MapAggregatorBase<>(mr);
  }

  public <R> MapAggregatorBase<U, R> map(SerializableFunction<X, R> mapper) {
    return with(mr.flatMap(entryMap(mapper)));
  }

  public <R> MapAggregatorBase<U, R> flatMap(SerializableFunction<X, Stream<R>> mapper) {
    return with(mr.flatMap(entryFlatMap(mapper)));
  }

  public <R> MapAggregatorBase<U, R> flatMapIterable(SerializableFunction<X, Iterable<R>> mapper) {
    return flatMap(x -> Streams.stream(mapper.apply(x)));
  }

  public MapAggregatorBase<U, X> filter(SerializablePredicate<X> predicate) {
    return with(mr.filter(entryFilter(predicate)));
  }

  public <V> MapAggregatorBase<CombinedIndex<U, V>, X> aggregateBy(SerializableFunction<X, V> indexer) {
    return with(mr.map(ux ->
        entry(new CombinedIndex<>(ux.getKey(), indexer.apply(ux.getValue())), ux.getValue())));
  }

  /**
   * Sets up aggregation by a custom time index.
   *
   * <p>The timestamps returned by the supplied indexing function are matched to the corresponding
   * time intervals</p>
   *
   * @param indexer a callback function that returns a timestamp object for each given data.
   *                Note that if this function returns timestamps outside of the supplied
   *                timestamps() interval results may be undefined
   * @return a MapAggregatorByTimestampAndIndex object with the equivalent state (settings,
   *         filters, map function, etc.) of the current MapReducer object
   */
  public MapAggregatorBase<CombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(mr.view.getTimestamps().get());
    final OSHDBTimestamp minTime = timestamps.first();
    final OSHDBTimestamp maxTime = timestamps.last();
    return this.aggregateBy(x -> {
      // match timestamps to the given timestamp list
      OSHDBTimestamp aggregationTimestamp = indexer.apply(x);
      if (aggregationTimestamp == null
          || aggregationTimestamp.compareTo(minTime) < 0
          || aggregationTimestamp.compareTo(maxTime) > 0) {
        throw new OSHDBInvalidTimestampException(
            "Aggregation timestamp outside of time query interval.");
      }
      return timestamps.floor(aggregationTimestamp);
    });
  }


  public MapReducerBase<Entry<U, X>> getMapReducer() {
    return mr;
  }

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

  public <S> Map<U, S>  reduce(Function<MapAggregatorBase<U, X>, Map<U, S>> reducer) {
    return reducer.apply(this);
  }

  public <S> Map<U, S> collect(Function<MapAggregatorBase<U, X>, Map<U, S>> collector) {
    return collector.apply(this);
  }

  public Stream<Entry<U, X>> stream() {
    return mr.stream();
  }

  public void forEach(BiConsumer<U, X> consumer) {
    stream().forEach(entry -> consumer.accept(entry.getKey(), entry.getValue()));
  }

  public Map<U, Long> count() {
    return map(x -> 1).reduce(Reduce::sumInt);
  }

  protected <R> SerializableFunction<Entry<U, X>, Stream<Entry<U,R>>> entryMap(SerializableFunction<X, R> map) {
    return ux -> ofNullable(map.apply(ux.getValue())).map(r -> entry(ux.getKey(), r)).stream();
  }

  protected <R> SerializableFunction<Entry<U, X>, Stream<Entry<U, R>>> entryFlatMap(SerializableFunction<X, Stream<R>> map){
    return ux -> map.apply(ux.getValue())
          .filter(Objects::nonNull)
          .map(r -> entry(ux.getKey(), r));
  }

  protected SerializablePredicate<Entry<U, X>> entryFilter(SerializablePredicate<X> predicate) {
    return ux -> predicate.test(ux.getValue());
  }
}
