package org.heigit.bigspatialdata.oshdb.api.db;

import java.io.File;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteRunnable;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerIgniteAffinityCall;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerIgniteLocalPeek;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerIgniteScanQuery;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;

/**
 * OSHDB database backend connector to a Ignite system.
 */
public class OSHDBIgnite extends OSHDBDatabase implements AutoCloseable {
  public enum ComputeMode {
    LocalPeek,
    ScanQuery,
    AffinityCall
  }

  private final transient Ignite ignite;
  private ComputeMode computeMode = ComputeMode.LocalPeek;
  private Long timeout = null;

  private IgniteRunnable onCloseCallback = null;

  public OSHDBIgnite() {
    this(new File("ignite-config.xml"));
  }

  public OSHDBIgnite(Ignite ignite) {
    this.ignite = ignite;
    this.ignite.cluster().active(true);
  }

  public OSHDBIgnite(String igniteConfigFilePath) {
    this(new File(igniteConfigFilePath));
  }

  /**
   * Opens a connection to oshdb data stored on an Ignite cluster.
   *
   * @param igniteConfig ignite configuration file
   */
  public OSHDBIgnite(File igniteConfig) {
    Ignition.setClientMode(true);

    this.ignite = Ignition.start(igniteConfig.toString());
    this.ignite.cluster().active(true);
  }

  @Override
  public OSHDBIgnite prefix(String prefix) {
    return (OSHDBIgnite) super.prefix(prefix);
  }

  @Override
  public <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
    MapReducer<X> mapReducer;
    Collection<String> allCaches = this.getIgnite().cacheNames();
    Collection<String> expectedCaches = Stream.of(OSMType.values())
        .map(TableNames::forOSMType).filter(Optional::isPresent).map(Optional::get)
        .map(t -> t.toString(this.prefix()))
        .collect(Collectors.toList());
    if (!allCaches.containsAll(expectedCaches)) {
      throw new OSHDBTableNotFoundException(StringUtils.join(expectedCaches, ", "));
    }
    switch (this.computeMode()) {
      case LocalPeek:
        mapReducer = new MapReducerIgniteLocalPeek<X>(this, forClass);
        break;
      case ScanQuery:
        mapReducer = new MapReducerIgniteScanQuery<X>(this, forClass);
        break;
      case AffinityCall:
        mapReducer = new MapReducerIgniteAffinityCall<X>(this, forClass);
        break;
      default:
        throw new UnsupportedOperationException("Backend not implemented for this compute mode.");
    }
    return mapReducer;
  }

  @Override
  public String metadata(String property) {
    // todo: implement this
    return null;
  }

  public Ignite getIgnite() {
    return this.ignite;
  }

  public void close() {
    this.ignite.close();
  }

  /**
   * Sets the compute mode.
   *
   * @param computeMode the compute mode to be used in calculations on this oshdb backend
   * @return this backend
   */
  public OSHDBIgnite computeMode(ComputeMode computeMode) {
    this.computeMode = computeMode;
    return this;
  }

  /**
   * Gets the set compute mode.
   *
   * @return the currently set compute mode
   */
  public ComputeMode computeMode() {
    return this.computeMode;
  }

  /**
   * Set a timeout for queries on this ignite oshdb backend.
   *
   * <p>If a query takes longer than the given time limit, a {@link OSHDBTimeoutException} will be
   * thrown.</p>
   *
   * @param seconds time (in seconds) a query is allowed to run for.
   * @return the current oshdb object
   */
  public OSHDBIgnite timeout(double seconds) {
    if (this.computeMode() == ComputeMode.ScanQuery) {
      throw new UnsupportedOperationException("Query timeouts not implemented in ScanQuery mode");
    }
    this.timeout = (long) Math.ceil(seconds * 1000);
    return this;
  }

  /**
   * Set a timeout for queries on this ignite oshdb backend.
   *
   * <p>If a query takes longer than the given time limit, a {@link OSHDBTimeoutException} will be
   * thrown.</p>
   *
   * @param milliSeconds time (in milliseconds) a query is allowed to run for.
   * @return the current oshdb object
   */
  public OSHDBIgnite timeoutInMilliseconds(long milliSeconds) {
    if (this.computeMode() == ComputeMode.ScanQuery) {
      throw new UnsupportedOperationException("Query timeouts not implemented in ScanQuery mode");
    }
    this.timeout = milliSeconds;
    return this;
  }

  /**
   * Gets the timeout for queries on this ignite oshdb backend, if present.
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

  /**
   * Sets a callback to be executed on all ignite workers after the query has been finished.
   *
   * <p>This can be used to close connections to (temporary) databases that were used to store or
   * retrieve intermediate data.</p>
   *
   * @param action the callback to execute after a query is done
   * @return the current oshdb database object
   */
  public OSHDBIgnite onClose(IgniteRunnable action) {
    this.onCloseCallback = action;
    return this;
  }


  /**
   * Gets the onClose callback.
   *
   * @return the currently set onClose callback
   */
  public Optional<IgniteRunnable> onClose() {
    if (this.onCloseCallback == null) {
      return Optional.empty();
    } else {
      return Optional.of(this.onCloseCallback);
    }
  }
}
