package org.heigit.ohsome.oshdb.api.mapreducer.base;

import static java.util.Map.entry;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Collector;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Reduce;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;

public class MapReducerBase<X> {
  protected final OSHDBView<?> view;
  protected final OSHDBDatabase oshdb;
  public final SerializableFunction<OSHEntity, Stream<X>> transform;

  public MapReducerBase(OSHDBView<?> view,
      OSHDBDatabase oshdb,
      SerializableFunction<OSHEntity, Stream<X>> transform) {
    this.view = view;
    this.oshdb = oshdb;
    this.transform = transform;
  }

  public OSHDBView<?> view(){
    return view;
  }

  private <R> MapReducerBase<R> with(SerializableFunction<OSHEntity, Stream<R>> transform) {
    return new MapReducerBase<>(view, oshdb, transform);
  }

  public <R> MapReducerBase<R> map(SerializableFunction<X, R> mapper) {
    return with(apply(sx -> sx.map(mapper)));
  }

  public <R> MapReducerBase<R> flatMap(SerializableFunction<X, Stream<R>> mapper) {
    return with(apply(sx -> sx.flatMap(mapper)));
  }

  public <R> MapReducerBase<R> flatMapIterable(SerializableFunction<X, Iterable<R>> mapper) {
    return flatMap(x -> Streams.stream(mapper.apply(x)));
  }

  public MapReducerBase<X> filter(SerializablePredicate<X> predicate) {
    return with(apply(x -> x.filter(predicate)));
  }

  public <U> MapAggregatorBase<U, X> aggregateBy(SerializableFunction<X, U> indexer) {
    return new MapAggregatorBase<>(map(x -> entry(indexer.apply(x), x)));
  }

  /**
   * Sets up aggregation by a custom time index.
   *
   * <p>The timestamps returned by the supplied indexing function are matched to the corresponding
   * time intervals.</p>
   *
   * @param indexer a callback function that return a timestamp object for each given data. Note
   *                that if this function returns timestamps outside of the supplied timestamps()
   *                interval results may be undefined
   * @return a MapAggregator object with the equivalent state (settings,
   *         filters, map function, etc.) of the current MapReducer object
   */
  public MapAggregatorBase<OSHDBTimestamp, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(view.getTimestamps().get());
    final OSHDBTimestamp minTime = timestamps.first();
    final OSHDBTimestamp maxTime = timestamps.last();
    return aggregateBy(x -> {
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

  public <U> MapAggregatorBase<U, X> aggregate(Function<MapReducerBase<X>, MapAggregatorBase<U, X>> agg) {
    return agg.apply(this);
  }


  public <R> MapReducerBase<R> mapBase(SerializableBiFunction<OSHEntity, X, R> mapper) {
    throw new UnsupportedOperationException();
  }

  public <R> MapReducerBase<R> flatMapBase(SerializableBiFunction<OSHEntity, X, Stream<R>> mapper) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  public MapReducerBase<List<X>> groupByEntity(){
    return new MapReducerBase<>(view, oshdb,
        osh -> Stream.of(transform.apply(osh).collect(toList())).filter(not(List::isEmpty)));
  }

  public <R> MapReducerBase<R> groupByEntity(SerializableBiFunction<OSHEntity, Stream<X>, Stream<R>> mapper) {
    return new MapReducerBase<>(view, oshdb, osh -> mapper.apply(osh, transform.apply(osh)));
  }

  protected <R> SerializableFunction<OSHEntity, Stream<R>> apply(
      SerializableFunction<Stream<X>, Stream<R>> fnt) {
    return o -> fnt.apply(transform.apply(o));
  }

  public <S> S reduce(
      SerializableSupplier<S> identity,
      SerializableBiFunction<S, X, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return oshdb.query(view,
        osh -> transform.apply(osh).reduce(identity.get(), accumulator, combiner),
        identity.get(), combiner, combiner);
  }

  //  public X reduce(SerializableSupplier<X> identity, SerializableBinaryOperator<X> accumulator) {
  //    return this.reduce(identity, accumulator::apply, accumulator);
  //  }

  public <S> S reduce(Function<MapReducerBase<X>, S> reducer) {
    return reducer.apply(this);
  }

  public <S> S collect(Collector<X, S> collector) {
    return collector.apply(this);
  }

  public Stream<X> stream() {
    return oshdb.query(view, osh -> transform.apply(osh));
  }

  public void forEach(Consumer<X> consumer) {
    stream().forEach(consumer);
  }

  public long count() {
    return Reduce.sumInt(map(x -> 1L));
  }
}
