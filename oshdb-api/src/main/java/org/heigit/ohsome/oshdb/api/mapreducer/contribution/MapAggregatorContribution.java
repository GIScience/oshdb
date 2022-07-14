package org.heigit.ohsome.oshdb.api.mapreducer.contribution;

import static java.util.Map.entry;

import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorBase;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;

public class MapAggregatorContribution<U, X> extends MapAggregatorBase<OSMContribution, U, X> {

  //private final MapReducerContribution<Entry<U, X>> mr;

  MapAggregatorContribution(MapReducerBase<OSMContribution, Entry<U, X>> mr) {
    super(mr);
  }

  private <V, R> MapAggregatorContribution<V, R> with(MapReducerBase<OSMContribution, Entry<V, R>> mr) {
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
  public MapAggregatorContribution<U, X> filter(SerializablePredicate<X> predicate) {
    return with(mr.filter(entryFilter(predicate)));
  }

  @Override
  public <V> MapAggregatorContribution<CombinedIndex<U, V>, X> aggregateBy(SerializableFunction<X, V> indexer) {
    return with(mr.map(ux -> entry(new CombinedIndex<U, V>(ux.getKey(), indexer.apply(ux.getValue())), ux.getValue())));
  }

  public MapAggregatorContribution<CombinedIndex<U, OSHDBTimestamp>, X> aggregateByTimestamp() {
    return with(mr.mapBase((s, ux) -> entry(new CombinedIndex(ux.getKey(), s.getTimestamp()), ux.getValue())));
  }

}
