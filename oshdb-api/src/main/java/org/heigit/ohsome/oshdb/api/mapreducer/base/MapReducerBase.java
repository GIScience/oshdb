package org.heigit.ohsome.oshdb.api.mapreducer.base;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.mapreducer.NewMapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;

public abstract class MapReducerBase<B, X> implements NewMapReducer<X> {
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

  public abstract <R> MapReducerBase<B, R> mapBase(SerializableBiFunction<B, X, R> mapper);

  public abstract <R> MapReducerBase<B, R> flatMapBase(SerializableBiFunction<B, X, Stream<R>> mapper);


  public MapReducerOSHEntity<List<X>> groupByEntity(){
    return new MapReducerOSHEntity<>(view,
        osh -> Stream.of(base.apply(osh).flatMap(transform).collect(toList())));
  }

  protected <R> SerializableFunction<B, Stream<R>> apply(
      SerializableFunction<Stream<X>, Stream<R>> fnt) {
    return o -> fnt.apply(transform.apply(o));
  }

  @Override
  public <S> S reduce(SerializableSupplier<S> identity,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) {
    var transformer = base.andThen(b -> b.flatMap(transform));
    return view.oshdb.query(view,
        entities -> entities.flatMap(transformer).reduce(identity.get(), accumulator, combiner),
        identity.get(), combiner, combiner);
  }

  @Override
  public Stream<X> stream() {
    var transformer = base.andThen(b -> b.flatMap(transform));
    return view.oshdb.query(view, entities -> entities.flatMap(transformer));
  }
}
