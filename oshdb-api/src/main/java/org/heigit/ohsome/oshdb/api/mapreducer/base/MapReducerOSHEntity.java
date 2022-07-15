package org.heigit.ohsome.oshdb.api.mapreducer.base;

import com.google.common.collect.Streams;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;

public class MapReducerOSHEntity<X> extends MapReducerBase<OSHEntity, X> {


  MapReducerOSHEntity(OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<X>> transform) {
    this(view, Stream::of, transform);
  }

  private MapReducerOSHEntity(OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<OSHEntity>> base,
      SerializableFunction<OSHEntity, Stream<X>> transform) {
    super(view, base, transform);
  }

  private <R> MapReducerOSHEntity<R> with(SerializableFunction<OSHEntity, Stream<R>> transform) {
    return new MapReducerOSHEntity<>(view, base, transform);
  }

  @Override
  public <R> MapReducerOSHEntity<R> map(SerializableFunction<X, R> map) {
    return with(apply(sx -> sx.map(map)));
  }

  @Override
  public <R> MapReducerOSHEntity<R> flatMap(SerializableFunction<X, Stream<R>> mapper) {
    return with(apply(sx -> sx.flatMap(mapper)));
  }

  @Override
  public <R> MapReducerBase<OSHEntity, R> flatMapIterable(SerializableFunction<X, Iterable<R>> mapper) {
    return flatMap(x -> Streams.stream(mapper.apply(x)));
  }

  @Override
  public MapReducerOSHEntity<X> filter(SerializablePredicate<X> predicate) {
    return with(apply(x -> x.filter(predicate)));
  }

  @Override
  public <R> MapReducerOSHEntity<R> mapBase(
      SerializableBiFunction<OSHEntity, X, R> mapper) {
    return with(s -> transform.apply(s).map(x -> mapper.apply(s, x)));
  }

  @Override
  public <R> MapReducerOSHEntity<R> flatMapBase(
      SerializableBiFunction<OSHEntity, X, Stream<R>> mapper) {
    return with(s -> transform.apply(s).flatMap(x -> mapper.apply(s, x)));
  }

  @Override
  public <U> MapAggregatorOSHEntity<U, X> aggregateBy(SerializableFunction<X, U> indexer) {
    return null;
  }
}
