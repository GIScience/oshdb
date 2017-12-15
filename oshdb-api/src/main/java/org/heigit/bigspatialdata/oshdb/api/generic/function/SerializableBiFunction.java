package org.heigit.bigspatialdata.oshdb.api.generic.function;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBiFunction<T1, T2, R> extends BiFunction<T1, T2, R>, Serializable {
}
