package org.heigit.ohsome.oshdb.api.mapreducer.base;

import com.google.common.collect.Streams;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;

public class MapReducerOSHEntity extends MapReducerBase<OSHEntity> {


  MapReducerOSHEntity(OSHDBView<?> view,
      OSHDBDatabase oshdb,
      SerializableFunction<OSHEntity, Stream<OSHEntity>> transform) {
    super(view, oshdb, transform);
  }

  private MapReducerOSHEntity with(SerializableFunction<OSHEntity, Stream<OSHEntity>> transform) {
    return new MapReducerOSHEntity(view, oshdb, transform);
  }

  @Override
  public MapReducerOSHEntity filter(SerializablePredicate<OSHEntity> predicate) {
    return with(apply(x -> x.filter(predicate)));
  }

  @Override
  public <U> MapAggregatorOSHEntity<U> aggregateBy(SerializableFunction<OSHEntity, U> indexer) {
    return null;
  }
}
