package org.heigit.bigspatialdata.oshdb.api.generic.function;

import java.io.Serializable;
import java.util.function.Supplier;

// interfaces of some generic lambda functions used here, to make them serializable
public interface SerializableSupplier<R> extends Supplier<R>, Serializable {
}
