package org.heigit.bigspatialdata.oshdb.api.generic.function;

import java.io.Serializable;
import java.util.function.Function;

public interface SerializableFunctionWithException<T, R> extends Serializable {
    R apply(T t) throws Exception;
}
