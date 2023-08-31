package org.heigit.ohsome.oshdb.api.mapreducer;

import java.util.Collections;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;

/**
 * A special map function that represents a filter.
 *
 * <p>Note that this class is using raw types on purpose because MapReducer's "map functions"
 * are designed to input and output arbitrary data types. The necessary type checks are performed
 * at at runtime in the respective setters.</p>
 */
@SuppressWarnings({"rawtypes", "unchecked"}) // see javadoc above
class FilterFunction extends MapFunction {
  private final SerializablePredicate filter;

  FilterFunction(SerializablePredicate filter) {
    super((x, ignored) -> filter.test(x)
        ? Collections.singletonList(x)
        : Collections.emptyList(),
        true);
    this.filter = filter;
  }

  public boolean test(Object root) {
    return this.filter.test(root);
  }
}
