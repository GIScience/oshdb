package org.heigit.ohsome.oshdb.api.mapreducer.snapshot;

import static java.util.Map.entry;
import java.util.Map;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;

public class MapReducerSnapshot<X> extends MapReducerBase<OSMEntitySnapshot, X> {

  public MapReducerSnapshot(OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<OSMEntitySnapshot>> base,
      SerializableFunction<OSMEntitySnapshot, Stream<X>> transform) {
    super(view, base, transform);
  }

  private <R> MapReducerSnapshot<R> with(SerializableFunction<OSMEntitySnapshot, Stream<R>> transform){
    return new MapReducerSnapshot<>(view, base, transform);
  }

  @Override
  public <R> MapReducerSnapshot<R> map(SerializableFunction<X, R> map) {
    return with(apply(sx -> sx.map(map)));
  }

  @Override
  public <R> MapReducerSnapshot<R> flatMap(SerializableFunction<X, Stream<R>> map) {
    return with(apply(sx -> sx.flatMap(map)));
  }

  @Override
  public MapReducerSnapshot<X> filter(SerializablePredicate<X> f) {
    return with(apply(x -> x.filter(f)));
  }

  @Override
  public <R> MapReducerSnapshot<R> mapBase(SerializableBiFunction<OSMEntitySnapshot, X, R> indexer){
    return with(s -> transform.apply(s).map(x -> indexer.apply(s, x)));
  }

  @Override
  public <R> MapReducerSnapshot<R> flatMapBase(SerializableBiFunction<OSMEntitySnapshot, X, Stream<R>> map) {
    return with(s -> transform.apply(s).flatMap(x -> map.apply(s, x)));
  }

  @Override
  public <U> MapAggregatorSnapshot<U, X> aggregateBy(SerializableFunction<X, U> indexer) {
    return new MapAggregatorSnapshot<>(map(x -> Map.entry(indexer.apply(x), x)));
  }

  public MapAggregatorSnapshot<OSHDBTimestamp, X> aggregateByTimestamp() {
    return new MapAggregatorSnapshot<>(mapBase((s, x) -> entry(s.getTimestamp(), x)));
  }
}
