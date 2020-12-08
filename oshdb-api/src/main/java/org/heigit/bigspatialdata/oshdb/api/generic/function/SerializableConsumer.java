package org.heigit.bigspatialdata.oshdb.api.generic.function;

import java.io.Serializable;
import java.util.function.Consumer;

public interface SerializableConsumer<T> extends Consumer<T>, Serializable {
}
