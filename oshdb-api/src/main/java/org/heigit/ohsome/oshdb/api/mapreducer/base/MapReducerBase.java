package org.heigit.ohsome.oshdb.api.mapreducer.base;

import static java.util.stream.Collectors.toList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;

public abstract class MapReducerBase<B, X> {
  protected final OSHDBView<?> view;
  protected final SerializableFunction<OSHEntity, Stream<B>> base;
  public final SerializableFunction<B, Stream<X>> transform;

  protected MapReducerBase(OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<B>> base,
      SerializableFunction<B, Stream<X>> transform) {
    this.view = view;
    this.base = base;
    this.transform = transform;
  }

  public abstract <R> MapReducerBase<B, R> map(SerializableFunction<X, R> mapper);

  public abstract <R> MapReducerBase<B, R> flatMap(SerializableFunction<X, Stream<R>> mapper);

  public abstract <R> MapReducerBase<B, R> flatMapIterable(SerializableFunction<X, Iterable<R>> mapper);

  public abstract MapReducerBase<B, X> filter(SerializablePredicate<X> predicate);

  public abstract <U> MapAggregatorBase<B, U, X> aggregateBy(SerializableFunction<X, U> indexer);

  public abstract <R> MapReducerBase<B, R> mapBase(SerializableBiFunction<B, X, R> mapper);

  public abstract <R> MapReducerBase<B, R> flatMapBase(SerializableBiFunction<B, X, Stream<R>> mapper);

  @Deprecated
  public MapReducerOSHEntity<List<X>> groupByEntity(){
    return new MapReducerOSHEntity<>(view,
        osh -> Stream.of(base.apply(osh).flatMap(transform).collect(toList())));
  }

  public <R> MapReducerOSHEntity<R> groupByEntity(SerializableFunction<Stream<X>, Stream<R>> mapper) {
    return new MapReducerOSHEntity<>(view, osh -> mapper.apply(base.apply(osh).flatMap(transform)));
  }

  protected <R> SerializableFunction<B, Stream<R>> apply(
      SerializableFunction<Stream<X>, Stream<R>> fnt) {
    return o -> fnt.apply(transform.apply(o));
  }

  public <S> S reduce(SerializableSupplier<S> identity,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
    return view.oshdb.query(view, osh -> base.apply(osh).flatMap(transform)
        .reduce(identity.get(), accumulator, combiner),
        identity.get(), combiner, combiner);
  }

  public X reduce(SerializableSupplier<X> identity, SerializableBinaryOperator<X> accumulator) {
    return this.reduce(identity, accumulator::apply, accumulator);
  }

  public <S> S reduce(Function<MapReducerBase<B, X>, S> reducer) {
    return reducer.apply(this);
  }

  public Stream<X> stream() {
    return view.oshdb.query(view, osh -> base.apply(osh).flatMap(transform));
  }

  public void forEach(Consumer<X> consumer) {
    stream().forEach(consumer);
  }

  public long count() {
    return Agg.sumLong(map(x -> 1L));
  }

  public Set<X> uniq() {
    return Agg.uniq(this);
  }

  public int countUniq() {
    return uniq().size();
  }
}
