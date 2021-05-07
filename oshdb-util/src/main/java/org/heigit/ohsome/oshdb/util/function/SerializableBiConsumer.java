package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.BiConsumer;

public interface SerializableBiConsumer<T1, T2> extends BiConsumer<T1, T2>, Serializable {}
