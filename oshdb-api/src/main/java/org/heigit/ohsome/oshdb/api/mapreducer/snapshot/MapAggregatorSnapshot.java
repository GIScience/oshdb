package org.heigit.ohsome.oshdb.api.mapreducer.snapshot;

import static java.util.Map.entry;

import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorBase;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;

public class MapAggregatorSnapshot<U, X> extends MapAggregatorBase<OSMEntitySnapshot, U, X> {

  private final MapReducerSnapshot<Entry<U, X>> mr;

  MapAggregatorSnapshot(MapReducerSnapshot<Entry<U,X>> mr) {
    this.mr = mr;
  }

  private <V, R> MapAggregatorSnapshot<V, R> with(MapReducerSnapshot<Entry<V, R>> mr) {
    return new MapAggregatorSnapshot<>(mr);
  }

  @Override
  public <R> MapAggregatorSnapshot<U, R> map(SerializableFunction<X, R> map) {
    return with(mr.map(entryMap(map)));
  }

  @Override
  public <R> MapAggregatorSnapshot<U, R> flatMap(SerializableFunction<X, Stream<R>> map) {
    return with(mr.flatMap(entryFlatMap(map)));
  }

  @Override
  public <V> MapAggregatorSnapshot<CombinedIndex<U, V>, X> aggregateBy(
      SerializableFunction<X, V> indexer) {
    return with(mr.map(
        ux ->
        entry(new CombinedIndex<U, V>(ux.getKey(), indexer.apply(ux.getValue())), ux.getValue())));
  }

  public MapAggregatorSnapshot<CombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
    return with(mr.mapBase((s, ux) -> entry(new CombinedIndex(ux.getKey(), s.getTimestamp()), ux.getValue())));
  }
}
