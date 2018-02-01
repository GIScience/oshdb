package org.heigit.bigspatialdata.oshdb.tool.importer.util.lambda;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class ConsumerUtil {

  public interface ThrowingConsumer<T, E extends Exception> {
    void accept(T t) throws E;
  }

  public interface ThrowingBiConsumer<T, U, E extends Exception> {
    void accept(T t, U u) throws E;
  }

  public static <T> Consumer<T> throwingConsumer(ThrowingConsumer<T, Exception> consumer) {
    return i -> {
      try {
        consumer.accept(i);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  public static <T, E extends Exception> Consumer<T> throwingConsumer(ThrowingConsumer<T, Exception> consumer,
      Class<E> clazz, ThrowingBiConsumer<T, E, Exception> exceptionHandler) {
    return i -> {
      try {
        consumer.accept(i);
      } catch (Exception ex) {
        try {
          E exCast = clazz.cast(ex);
          exceptionHandler.accept(i, exCast);
        } catch (Exception ccEx) {
          throw new RuntimeException(ex);
        }
      }
    };
  }
}
