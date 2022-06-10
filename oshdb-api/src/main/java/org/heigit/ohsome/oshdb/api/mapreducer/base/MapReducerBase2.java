package org.heigit.ohsome.oshdb.api.mapreducer.base;

import static java.util.Map.entry;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;

public abstract class MapReducerBase2<T, X> implements MapReducer<X> {

  protected final OSHDBDatabase oshdb;
  protected final OSHDBView<?> view;
  protected final SerializableFunction<T, Stream<X>> transform;

  protected MapReducerBase2(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<T, Stream<X>> transform) {
    this.oshdb = oshdb;
    this.view = view;
    this.transform = transform;
  }

  protected abstract <R> MapReducerBase2<T, R> with(SerializableFunction<T, Stream<R>> transform);

  protected abstract <U extends Comparable<U> & Serializable, R>
      MapAggregatorBase2<T, U, R> aggregator(
          SerializableFunction<T, Stream<Map.Entry<U, R>>> transform);

  @Override
  public <R> MapReducerBase2<T, R> map(SerializableFunction<X, R> map) {
    return with(transform.andThen(x -> x.map(map)));
  }

  @Override
  public <R> MapReducerBase2<T, R> flatMap(SerializableFunction<X, Stream<R>> map) {
    return with(transform.andThen(x -> x.flatMap(map)));
  }

  @Override
  public MapReducerBase2<T, X> filter(SerializablePredicate<X> f) {
    return with(transform.andThen(x -> x.filter(f)));
  }

  @Override
  public <U extends Comparable<U> & Serializable> MapAggregatorBase2<T, U, X> aggregateBy(
      SerializableFunction<X, U> indexer) {
    return aggregator(transform.andThen(s -> s.map(x -> entry(indexer.apply(x), x))));
  }

  @Override
  public MapAggregatorBase2<T, OSHDBTimestamp, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    indexTimestamp(view.getTimestamps().get(), indexer);
    return aggregateBy(indexTimestamp(view.getTimestamps().get(), indexer));
  }

  protected abstract SerializableFunction<OSHEntity, Stream<X>> getTransformer();

  @Override
  public <S> S reduce(SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
    var transformer = getTransformer();
    return oshdb.query(view,
        entities -> entities.flatMap(transformer)
            .reduce(identitySupplier.get(), accumulator, combiner),
        identitySupplier.get(), combiner, combiner);
  }

  @Override
  public Stream<X> stream() {
    var transformer = getTransformer();
    return oshdb.query(view, entities -> entities.flatMap(transformer));
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
