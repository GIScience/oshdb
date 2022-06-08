package org.heigit.ohsome.oshdb.api.db;

import java.util.OptionalLong;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;

/**
 * OSHDB database backend connector.
 */
public abstract class OSHDBDatabase implements AutoCloseable {
  private String prefix = "";
  private Long timeout = null;

  /**
   * Factory function that creates a mapReducer object of the appropriate data type class for this
   * oshdb backend implemenation.
   *
   * @param view the view to iterate over in the `mapping` function of the generated
   *        MapReducer
   * @return a new mapReducer object operating on the given OSHDB backend
   */
  public abstract <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(OSHDBView<X> view)
      throws OSHDBException;

  /**
   * Returns metadata about the given OSHDB.
   *
   * <p>For example copyright information, currentness of the data, spatial extent, etc.</p>
   */
  public abstract String metadata(String property);

  /**
   * Sets the "table/cache" name prefix to be used with this oshdb.
   */
  public OSHDBDatabase prefix(String prefix) {
    this.prefix = prefix;
    return this;
  }

  /**
   * Returns the currently set db "table/cache" name prefix.
   */
  public String prefix() {
    return this.prefix;
  }

  /**
   * Set a timeout for queries on this oshdb backend.
   *
   * <p>If a query takes longer than the given time limit, a {@link OSHDBTimeoutException} will be
   * thrown.</p>
   *
   * @param seconds time (in seconds) a query is allowed to run for.
   * @return the current oshdb object
   */
  public OSHDBDatabase timeout(double seconds) {
    return this.timeoutInMilliseconds((long) Math.ceil(seconds * 1000));
  }

  /**
   * Clears a previously set timeout for queries on this oshdb backend.
   *
   * @return the current oshdb object
   */
  public OSHDBDatabase clearTimeout() {
    this.timeout = null;
    return this;
  }

  /**
   * Set a timeout for queries on this oshdb backend.
   *
   * <p>If a query takes longer than the given time limit, a {@link OSHDBTimeoutException} will be
   * thrown.</p>
   *
   * @param milliSeconds time (in milliseconds) a query is allowed to run for.
   * @return the current oshdb object
   */
  public OSHDBDatabase timeoutInMilliseconds(long milliSeconds) {
    this.timeout = milliSeconds;
    return this;
  }

  /**
   * Gets the timeout for queries on this oshdb backend, if present.
   *
   * @return the currently set query timeout in milliseconds
   */
  public OptionalLong timeoutInMilliseconds() {
    if (this.timeout == null) {
      return OptionalLong.empty();
    } else {
      return OptionalLong.of(this.timeout);
    }
  }
}
