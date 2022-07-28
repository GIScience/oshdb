package org.heigit.ohsome.oshdb.api.mapreducer.snapshot;

import static java.util.Map.entry;
import com.google.common.collect.Streams;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.GeometrySplitter;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorBase;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.MapAggregatorContribution;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class MapAggregatorSnapshot<U> extends MapAggregatorBase<U, OSMEntitySnapshot> {

  MapAggregatorSnapshot(MapReducerBase<Entry<U, OSMEntitySnapshot>> mr) {
    super(mr);
  }

  private <V> MapAggregatorSnapshot<V> with(MapReducerBase<Entry<V, OSMEntitySnapshot>> mr) {
    return new MapAggregatorSnapshot<>(mr);
  }

  @Override
  public MapAggregatorBase<U, OSMEntitySnapshot> filter(SerializablePredicate<OSMEntitySnapshot> predicate) {
    return with(mr.filter(entryFilter(predicate)));
  }

  @Override
  public <V> MapAggregatorSnapshot<CombinedIndex<U, V>> aggregateBy(SerializableFunction<OSMEntitySnapshot, V> indexer) {
    return with(mr.map(ux -> entry(new CombinedIndex<>(ux.getKey(), indexer.apply(ux.getValue())), ux.getValue())));
  }

  public MapAggregatorSnapshot<CombinedIndex<U, OSHDBTimestamp>> aggregateByTimestamp() {
    return aggregateBy(OSMEntitySnapshot::getTimestamp);
  }

  public <V extends Comparable<V> & Serializable, P extends Geometry & Polygonal> MapAggregatorSnapshot<CombinedIndex<U, V>> aggregateByGeometry(Map<V, P> geometries) {
    var gs = new GeometrySplitter<>(geometries);
    return new MapAggregatorSnapshot<>(
        flatMap(x -> gs.split(x).entrySet().stream())
        .aggregateBy(Entry::getKey).map(Entry::getValue).getMapReducer());
  }
}
