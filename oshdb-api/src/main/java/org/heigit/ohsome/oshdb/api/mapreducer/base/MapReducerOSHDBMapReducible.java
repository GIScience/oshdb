package org.heigit.ohsome.oshdb.api.mapreducer.base;

import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;

public abstract class MapReducerOSHDBMapReducible<T extends OSHDBMapReducible, X>
    extends MapReducerBase2<T, X> {

  protected final SerializableFunction<OSHEntity, Stream<T>> toReducible;

  protected MapReducerOSHDBMapReducible(OSHDBDatabase oshdb, OSHDBView<?> view,
      SerializableFunction<OSHEntity, Stream<T>> toReducible,
      SerializableFunction<T, Stream<X>> transform) {
    super(oshdb, view, transform);
    this.toReducible = toReducible;
  }

  @Override
  protected SerializableFunction<OSHEntity, Stream<X>> getTransformer() {
    return osh -> toReducible.apply(osh).flatMap(transform);
  }
}
