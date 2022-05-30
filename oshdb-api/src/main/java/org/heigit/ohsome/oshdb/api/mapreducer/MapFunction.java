package org.heigit.ohsome.oshdb.api.mapreducer;

import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;

/**
 * A function that has a flag: <i>isFlatMapper</i>.
 *
 * <p>Note that this class is using raw types on purpose because MapReducer's "map functions"
 * are designed to input and output arbitrary data types. The necessary type checks are performed
 * at at runtime in the respective setters.</p>
 */
@SuppressWarnings({"rawtypes", "unchecked"}) // see javadoc above
class MapFunction implements SerializableBiFunction {
  private final SerializableBiFunction mapper;
  private final boolean isFlatMapper;

  MapFunction(SerializableBiFunction mapper, boolean isFlatMapper) {
    this.mapper = mapper;
    this.isFlatMapper = isFlatMapper;
  }

  boolean isFlatMapper() {
    return this.isFlatMapper;
  }

  @Override
  public Object apply(Object o, Object root) {
    return this.mapper.apply(o, root);
  }
}
