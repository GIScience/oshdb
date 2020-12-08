package org.heigit.bigspatialdata.oshdb.api.generic.function;

import java.io.Serializable;
import java.util.function.Predicate;

public interface SerializablePredicate<T> extends Predicate<T>, Serializable {
}
