package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.BinaryOperator;

public interface SerializableBinaryOperator<T> extends BinaryOperator<T>, Serializable {
}
