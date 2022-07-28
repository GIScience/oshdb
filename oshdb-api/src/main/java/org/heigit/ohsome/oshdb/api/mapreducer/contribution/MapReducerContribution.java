package org.heigit.ohsome.oshdb.api.mapreducer.contribution;

import static java.util.Map.entry;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.GeometrySplitter;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapAggregatorBase;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerBase;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class MapReducerContribution extends MapReducerBase<OSMContribution> {

  MapReducerContribution(OSHDBView<?> view, OSHDBDatabase oshdb,
      SerializableFunction<OSHEntity, Stream<OSMContribution>> transform) {
    super(view, oshdb, transform);
  }

  private MapReducerContribution with(SerializableFunction<OSHEntity, Stream<OSMContribution>> transform){
    return new MapReducerContribution(view, oshdb, transform);
  }

  @Override
  public MapReducerContribution filter(SerializablePredicate<OSMContribution> predicate) {
    return with(apply(x -> x.filter(predicate)));
  }

  @Override
  public <U> MapAggregatorContribution<U> aggregateBy(SerializableFunction<OSMContribution, U> indexer) {
    return new MapAggregatorContribution<>(map(x -> entry(indexer.apply(x), x)));
  }

  public MapAggregatorContribution<OSHDBTimestamp> aggregateByTimestamp() {
    var timestamps = view.getTimestamps().get();
    return new MapAggregatorContribution<>(map(x -> entry(timestamps.floor(x.getTimestamp()), x)));
  }

   @Override
  public MapAggregatorContribution<OSHDBTimestamp> aggregateByTimestamp(
      SerializableFunction<OSMContribution, OSHDBTimestamp> indexer) {
    return new MapAggregatorContribution<>(super.aggregateByTimestamp(indexer).getMapReducer());
  }

  public <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal> MapAggregatorContribution<U> aggregateByGeometry(Map<U, P> geometries) {
    var gs = new GeometrySplitter<>(geometries);
    return new MapAggregatorContribution<>(flatMap(x -> gs.split(x).entrySet().stream())
        .aggregateBy(Entry::getKey)
        .map(Entry::getValue).getMapReducer());
  }
}
