package org.heigit.ohsome.oshdb.api.mapreducer.contribution;

import static java.util.Map.entry;

import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorBase;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;

public class MapAggregatorContribution<U, X> extends MapAggregatorBase<OSMContribution, U, X> {

  private final MapReducerContribution<Entry<U, X>> mr;

  MapAggregatorContribution(MapReducerContribution<Entry<U, X>> mr) {
    this.mr = mr;
  }

  private <V, R> MapAggregatorContribution<V, R> with(MapReducerContribution<Entry<V, R>> mr) {
    return new MapAggregatorContribution<>(mr);
  }

  @Override
  public <R> MapAggregatorContribution<U, R> map(SerializableFunction<X, R> map) {
    return with(mr.map(entryMap(map)));
  }

  @Override
  public <R> MapAggregatorContribution<U, R> flatMap(SerializableFunction<X, Stream<R>> map) {
    return with(mr.flatMap(entryFlatMap(map)));
  }

  @Override
  public <V> MapAggregatorContribution<CombinedIndex<U, V>, X> aggregateBy(SerializableFunction<X, V> indexer) {
    return with(mr.map(ux -> entry(new CombinedIndex<U, V>(ux.getKey(), indexer.apply(ux.getValue())), ux.getValue())));
  }

  public MapAggregatorContribution<CombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
    return with(mr.mapBase((s, ux) -> entry(new CombinedIndex(ux.getKey(), s.getTimestamp()), ux.getValue())));
  }

}
