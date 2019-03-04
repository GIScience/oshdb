package org.heigit.bigspatialdata.oshdb.api.generic.function;

import java.io.Serializable;

public interface SerializableThrowingFunction<T, R> extends Serializable {
  R apply(T t) throws Exception;
}
