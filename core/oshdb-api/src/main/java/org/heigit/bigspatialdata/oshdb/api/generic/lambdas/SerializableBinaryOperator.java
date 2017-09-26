package org.heigit.bigspatialdata.oshdb.api.generic.lambdas;

import java.io.Serializable;
import java.util.function.BinaryOperator;

public interface SerializableBinaryOperator<T> extends BinaryOperator<T>, Serializable {
}
