package org.heigit.ohsome.oshdb.api.mapreducer.base;

import java.util.Map.Entry;
import org.heigit.ohsome.oshdb.api.mapreducer.CombinedIndex;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;

public class MapAggregatorOSHEntity<U> extends MapAggregatorBase<U, OSHEntity> {

  protected MapAggregatorOSHEntity(MapReducerBase<Entry<U, OSHEntity>> mr) {
    super(mr);
  }

  @Override
  public MapAggregatorOSHEntity<U> filter(SerializablePredicate<OSHEntity> predicate) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <V> MapAggregatorOSHEntity<CombinedIndex<U, V>> aggregateBy(
      SerializableFunction<OSHEntity, V> indexer) {
    return null;
  }

}
