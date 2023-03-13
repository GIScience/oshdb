package org.heigit.ohsome.oshdb.util.flux;

import java.util.Map.Entry;
import java.util.function.Function;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class FluxUtil {

  private FluxUtil() {}

  public static <T1, T2> Function<Entry<T1, T2>, Tuple2<T1, T2>> entryToTuple() {
    return entry -> Tuples.of(entry.getKey(), entry.getValue());
  }

  public static <T1,T2, R> Function<Tuple2<T1, T2>, Tuple2<T1,R>> mapT2(Function<T2, R> fnt) {
    return tuple -> tuple.mapT2(fnt);
  }

}
