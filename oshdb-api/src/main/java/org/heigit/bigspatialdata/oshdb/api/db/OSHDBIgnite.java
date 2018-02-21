package org.heigit.bigspatialdata.oshdb.api.db;

import java.io.File;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.OptionalLong;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerIgniteLocalPeek;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerIgniteScanQuery;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducerIgniteAffinityCall;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;

public class OSHDBIgnite extends OSHDBDatabase implements AutoCloseable, Serializable {
  public enum ComputeMode {
    LocalPeek,
    ScanQuery,
    AffinityCall
  }

  private final Ignite _ignite;
  private ComputeMode _computeMode = ComputeMode.LocalPeek;
  private Long _timeout = null;

  public OSHDBIgnite() throws SQLException, ClassNotFoundException {
    this(new File("ignite-config.xml"));
  }

  public OSHDBIgnite(Ignite ignite) {
    this._ignite = ignite;
    this._ignite.active(true);
  }

  public OSHDBIgnite(String igniteConfigFilePath) {
    this(new File(igniteConfigFilePath));
  }

  public OSHDBIgnite(File igniteConfig) {
    Ignition.setClientMode(true);

    this._ignite = Ignition.start(igniteConfig.toString());
    this._ignite.active(true);
  }

  @Override
  public <X extends OSHDBMapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
    MapReducer<X> mapReducer;
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
        throw new UnsupportedOperationException("Backend not implemented for this database option.");
    }
    return mapReducer;
  }

  @Override
  public String metadata(String property) {
    // todo: implement this
    return null;
  }

  public Ignite getIgnite() {
    return this._ignite;
  }

  public void close() {
    this._ignite.close();
  }

  public OSHDBIgnite computeMode(ComputeMode computeMode) {
    this._computeMode = computeMode;
    return this;
  }

  public ComputeMode computeMode() {
    return this._computeMode;
  }

  /**
   * Set a timeout for queries on this ignite oshdb backend.
   *
   * If a query takes longer than the given time limit, a {@link OSHDBTimeoutException} will be thrown.
   *
   * @param seconds time (in seconds) a query is allowed to run for.
   * @return the current oshdb object
   */
  public OSHDBIgnite timeout(double seconds) {
    if (this.computeMode() == ComputeMode.ScanQuery)
      throw new UnsupportedOperationException("Query timeouts not implemented in ScanQuery mode");
    this._timeout = (long)Math.ceil(seconds*1000);
    return this;
  }

  /**
   * Set a timeout for queries on this ignite oshdb backend.
   *
   * If a query takes longer than the given time limit, a {@link OSHDBTimeoutException} will be thrown.
   *
   * @param milliSeconds time (in milliseconds) a query is allowed to run for.
   * @return the current oshdb object
   */
  public OSHDBIgnite timeoutInMilliseconds(long milliSeconds) {
    if (this.computeMode() == ComputeMode.ScanQuery)
      throw new UnsupportedOperationException("Query timeouts not implemented in ScanQuery mode");
    this._timeout = milliSeconds;
    return this;
  }

  public OptionalLong timeoutInMilliseconds() {
    if (this._timeout == null)
      return OptionalLong.empty();
    else
      return OptionalLong.of(this._timeout);
  }
}
