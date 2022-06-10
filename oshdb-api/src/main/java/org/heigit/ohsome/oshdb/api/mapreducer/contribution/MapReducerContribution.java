package org.heigit.ohsome.oshdb.api.mapreducer.contribution;

import static java.util.Map.entry;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.MapAggregator;
import org.heigit.ohsome.oshdb.api.mapreducer.base.GeometrySplitter;
import org.heigit.ohsome.oshdb.api.mapreducer.base.MapReducerOSHDBMapReducible;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

public class MapReducerContribution<X> extends MapReducerOSHDBMapReducible<OSMContribution, X> {

  MapReducerContribution(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<OSMContribution>> toReducible,
      SerializableFunction<OSMContribution, Stream<X>> transform) {
    super(oshdb, view, toReducible, transform);
  }

  protected <R> MapReducerContribution<R> with(
      SerializableFunction<OSMContribution, Stream<R>> transform) {
    return new MapReducerContribution<>(oshdb, view, toReducible, transform);
  }

  @Override
  protected <U extends Comparable<U> & Serializable, R> MapAggregatorContribution<U, R> aggregator(
      SerializableFunction<OSMContribution, Stream<Entry<U, R>>> transform) {
    return new MapAggregatorContribution<>(oshdb, view, toReducible, transform);
  }

  @Override
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp() {
    var timestamps = view.getTimestamps().get();
    return aggregator(contrib -> transform.apply(contrib)
        .map(x -> entry(timestamps.floor(contrib.getTimestamp()), x)));
  }

  @Override
  public <U extends Comparable<U> & Serializable,
          P extends Geometry & Polygonal> MapAggregator<U, X> aggregateByGeometry(
      Map<U, P> geometries) {
    var gs = new GeometrySplitter<>(geometries);
    return aggregator(contrib -> gs.splitOSMContribution(contrib)
        .flatMap(us -> transform.apply(us.getValue()).map(x -> entry(us.getKey(), x))));
  }



}
