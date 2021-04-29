package org.heigit.ohsome.oshdb.util.function;

import java.io.Serializable;
import java.util.function.BiPredicate;

public interface SerializableBiPredicate<T, U> extends BiPredicate<T, U>, Serializable {

}
