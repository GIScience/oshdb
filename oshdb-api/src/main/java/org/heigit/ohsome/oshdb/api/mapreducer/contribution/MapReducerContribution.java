package org.heigit.ohsome.oshdb.api.mapreducer.contribution;

import static java.util.Map.entry;
import com.google.common.collect.Streams;
import java.util.Map;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;

public class MapReducerContribution<X> extends MapReducerBase<OSMContribution, X> {

  MapReducerContribution(OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<OSMContribution>> base,
      SerializableFunction<OSMContribution, Stream<X>> transform) {
    super(view, base, transform);
  }

  private <R> MapReducerContribution<R> with(SerializableFunction<OSMContribution, Stream<R>> transform){
    return new MapReducerContribution<>(this.view, this.base, transform);
  }

  @Override
  public <R> MapReducerContribution<R> map(SerializableFunction<X, R> map) {
    return with(apply(sx -> sx.map(map)));
  }

  @Override
  public <R> MapReducerContribution<R> flatMap(SerializableFunction<X, Stream<R>> mapper) {
    return with(apply(sx -> sx.flatMap(mapper)));
  }

  @Override
  public <R> MapReducerBase<OSMContribution, R> flatMapIterable(
      SerializableFunction<X, Iterable<R>> mapper) {
    return flatMap(x -> Streams.stream(mapper.apply(x)));
  }

  @Override
  public MapReducerContribution<X> filter(SerializablePredicate<X> predicate) {
    return with(apply(x -> x.filter(predicate)));
  }

  @Override
  public <R> MapReducerContribution<R> mapBase(
      SerializableBiFunction<OSMContribution, X, R> mapper) {
    return with(s -> transform.apply(s).map(x -> mapper.apply(s, x)));
  }

  @Override
  public <R> MapReducerContribution<R> flatMapBase(
      SerializableBiFunction<OSMContribution, X, Stream<R>> mapper) {
    return with(s -> transform.apply(s).flatMap(x -> mapper.apply(s, x)));
  }

  @Override
  public <U> MapAggregatorContribution<U, X> aggregateBy(SerializableFunction<X, U> indexer) {
    return new MapAggregatorContribution<>(map(x -> Map.entry(indexer.apply(x), x)));
  }

  public MapAggregatorContribution<OSHDBTimestamp, X> aggregateByTimestamp() {
    return new MapAggregatorContribution<>(mapBase((s, x) -> entry(s.getTimestamp(), x)));
  }
}
