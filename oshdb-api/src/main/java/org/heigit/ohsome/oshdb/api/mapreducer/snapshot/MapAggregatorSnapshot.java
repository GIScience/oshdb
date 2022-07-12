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

  @Override
  public <R> MapAggregatorSnapshot<U, R> map(SerializableFunction<X, R> map) {
    return new MapAggregatorSnapshot<>(mr.map(ux -> entry(ux.getKey(), map.apply(ux.getValue()))));
  }

  @Override
  public <R> MapAggregatorSnapshot<U, R> flatMap(SerializableFunction<X, Stream<R>> map) {
    return new MapAggregatorSnapshot<>(mr.flatMap(ux -> map.apply(ux.getValue()).map(r -> entry(ux.getKey(), r))));
  }

  @Override
  public <V> MapAggregatorSnapshot<CombinedIndex<U, V>, X> aggregateBy(
      SerializableFunction<X, V> indexer) {
    return new MapAggregatorSnapshot<>(mr.map(ux -> entry(new CombinedIndex<U, V>(ux.getKey(), indexer.apply(ux.getValue())), ux.getValue())));
  }

  public MapAggregatorSnapshot<CombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
    return new MapAggregatorSnapshot<>(mr.mapBase((s, ux) -> entry(new CombinedIndex(ux.getKey(), s.getTimestamp()), ux.getValue())));
  }
}
