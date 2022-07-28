package org.heigit.ohsome.oshdb.api.mapreducer.snapshot;

import static java.util.Map.entry;
import com.google.common.collect.Streams;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.api.mapreducer.GeometrySplitter;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.MapAggregatorContribution;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class MapReducerSnapshot extends MapReducerBase<OSMEntitySnapshot> {

  MapReducerSnapshot(OSHDBView<?> view, OSHDBDatabase oshdb,
      SerializableFunction<OSHEntity, Stream<OSMEntitySnapshot>> transform) {
    super(view, oshdb, transform);
  }

  private MapReducerSnapshot with(SerializableFunction<OSHEntity, Stream<OSMEntitySnapshot>> transform) {
    return new MapReducerSnapshot(view, oshdb, transform);
  }

  @Override
  public MapReducerSnapshot filter(SerializablePredicate<OSMEntitySnapshot> f) {
    return with(apply(x -> x.filter(f)));
  }

  @Override
  public <U> MapAggregatorSnapshot<U> aggregateBy(SerializableFunction<OSMEntitySnapshot, U> indexer) {
    return new MapAggregatorSnapshot<>(map(x -> entry(indexer.apply(x), x)));
  }

  public MapAggregatorSnapshot<OSHDBTimestamp> aggregateByTimestamp() {
    return aggregateBy(OSMEntitySnapshot::getTimestamp);
  }

  @Override
 public MapAggregatorSnapshot<OSHDBTimestamp> aggregateByTimestamp(
     SerializableFunction<OSMEntitySnapshot, OSHDBTimestamp> indexer) {
   return new MapAggregatorSnapshot<>(super.aggregateByTimestamp(indexer).getMapReducer());
 }

  public <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal> MapAggregatorSnapshot<U> aggregateByGeometry(Map<U, P> geometries) {
    var gs = new GeometrySplitter<>(geometries);
    return new MapAggregatorSnapshot<>(flatMap(x -> gs.split(x).entrySet().stream())
        .aggregateBy(Entry::getKey)
        .map(Entry::getValue).getMapReducer());
  }

}
