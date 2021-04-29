package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBiFunction<T1, T2, R> extends BiFunction<T1, T2, R>, Serializable {}
