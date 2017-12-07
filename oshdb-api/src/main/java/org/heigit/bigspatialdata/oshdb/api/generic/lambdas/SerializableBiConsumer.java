package org.heigit.bigspatialdata.oshdb.api.generic.lambdas;

import java.io.Serializable;
import java.util.function.BiConsumer;

public interface SerializableBiConsumer<T1, T2> extends BiConsumer<T1, T2>, Serializable {
}
