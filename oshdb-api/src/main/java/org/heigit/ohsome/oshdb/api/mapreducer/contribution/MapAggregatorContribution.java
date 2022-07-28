package org.heigit.ohsome.oshdb.api.mapreducer.contribution;

import static java.util.Map.entry;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.GeometrySplitter;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorBase;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class MapAggregatorContribution<U> extends MapAggregatorBase<U, OSMContribution> {

  //private final MapReducerContribution<Entry<U, X>> mr;

  MapAggregatorContribution(MapReducerBase<Entry<U, OSMContribution>> mr) {
    super(mr);
  }

  private <V> MapAggregatorContribution<V> with(MapReducerBase<Entry<V, OSMContribution>> mr) {
    return new MapAggregatorContribution<>(mr);
  }

  @Override
  public MapAggregatorContribution<U> filter(SerializablePredicate<OSMContribution> predicate) {
    return with(mr.filter(entryFilter(predicate)));
  }

  @Override
  public <V> MapAggregatorContribution<CombinedIndex<U, V>> aggregateBy(SerializableFunction<OSMContribution, V> indexer) {
    return with(mr.map(ux -> entry(new CombinedIndex<>(ux.getKey(), indexer.apply(ux.getValue())), ux.getValue())));
  }

  public MapAggregatorContribution<CombinedIndex<U, OSHDBTimestamp>> aggregateByTimestamp() {
    var timestamps = mr.view().getTimestamps().get();
    return new MapAggregatorContribution<>(aggregateBy(x -> timestamps.floor(x.getTimestamp())).getMapReducer());
  }

  public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal> MapAggregatorContribution<CombinedIndex<U, V>> aggregateByGeometry(Map<V, P> geometries) {
    var gs = new GeometrySplitter<>(geometries);
    return new MapAggregatorContribution<>(
        flatMap(x -> gs.split(x).entrySet().stream())
        .aggregateBy(Entry::getKey).map(Entry::getValue).getMapReducer());
  }
}
