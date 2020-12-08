package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;

/**
 * A function that has a flag: <i>isFlatMapper</i>.
 */
class MapFunction implements SerializableFunction {
  private SerializableFunction mapper;
  private boolean isFlatMapper;

  MapFunction(SerializableFunction mapper, boolean isFlatMapper) {
    this.mapper = mapper;
    this.isFlatMapper = isFlatMapper;
  }

  boolean isFlatMapper() {
    return this.isFlatMapper;
  }

  @Override
  @SuppressWarnings("unchecked")
  // mappers are using raw types because they work on arbitrary data types
  // the necessary type checks are done at the respective setters
  public Object apply(Object o) {
    return this.mapper.apply(o);
  }
}
