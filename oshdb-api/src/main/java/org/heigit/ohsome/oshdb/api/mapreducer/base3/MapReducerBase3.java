package org.heigit.ohsome.oshdb.api.mapreducer.base3;

import static java.util.Map.entry;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public abstract class MapReducerBase3<T, X> implements MapReducer<X> {

  protected final OSHDBDatabase oshdb;
  protected final OSHDBView<?> view;
  protected final SerializableFunction<OSHEntity, Stream<T>> toReducible;
  protected final SerializableFunction<T, Stream<X>> transform;

  protected MapReducerBase3(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<T>> toReducible,
      SerializableFunction<T, Stream<X>> transform) {
    this.oshdb = oshdb;
    this.view = view;
    this.toReducible = toReducible;
    this.transform = transform;
  }

  protected abstract <R> MapReducerBase3<T, R> with(SerializableFunction<T, Stream<R>> transform);

  protected abstract <U extends Comparable<U> & Serializable, R> MapAggregatorBase3<T, U, R>
      aggregator(SerializableFunction<T, Stream<Entry<U, R>>> transform);

  @Override
  public <R> MapReducer<R> map(SerializableFunction<X, R> map) {
    return with(transform.andThen(s -> s.map(map)));
  }

  @Override
  public <R> MapReducer<R> flatMap(SerializableFunction<X, Stream<R>> map) {
    return with(transform.andThen(s -> s.flatMap(map)));
  }

  @Override
  public MapReducer<X> filter(SerializablePredicate<X> f) {
    return with(transform.andThen(s -> s.filter(f)));
  }

  @Override
  public <U extends Comparable<U> & Serializable> MapAggregator<U, X> aggregateBy(
      SerializableFunction<X, U> indexer) {
    return aggregator(
        transform.andThen(s -> s.map(x -> entry(indexer.apply(x), x))));
  }

  @Override
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp()
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException("This view, don't support auto timestamp aggregation");
  }

  @Override
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer) {
    return aggregateBy(view.getTimestamps().indexTimestamp(indexer));
  }

  @Override
  public <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal>
      MapAggregator<U, X> aggregateByGeometry(Map<U, P> geometries) {
    throw new UnsupportedOperationException("This view, don't support auto geometry aggregation");
  }

  private SerializableFunction<OSHEntity, Stream<X>> transformer() {
    return osh -> toReducible.apply(osh).flatMap(transform);
  }

  @Override
  public <S> S reduce(SerializableSupplier<S> identity,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
    var transformer = transformer();
    return oshdb.query(view,
        entities -> entities.flatMap(transformer)
          .reduce(identity.get(), accumulator, combiner),
        identity.get(), combiner, combiner);
  }

  @Override
  public Stream<X> stream() {
    var transformer = transformer();
    return oshdb.query(view, entities -> entities.flatMap(transformer));
  }

}
