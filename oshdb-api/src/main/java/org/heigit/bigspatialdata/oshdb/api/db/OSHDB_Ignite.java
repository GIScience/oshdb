package org.heigit.bigspatialdata.oshdb.api.db;

import java.io.File;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.OptionalLong;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.heigit.bigspatialdata.oshdb.api.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_Ignite_LocalPeek;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_Ignite_ScanQuery;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_Ignite_AffinityCall;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDB_MapReducible;

public class OSHDB_Ignite extends OSHDB_Database implements AutoCloseable, Serializable {
  public enum ComputeMode {
    LocalPeek,
    ScanQuery,
    AffinityCall
  }

  private final Ignite _ignite;
  private ComputeMode _computeMode = ComputeMode.LocalPeek;
  private Long _timeout = null;

  public OSHDB_Ignite() throws SQLException, ClassNotFoundException {
    this(new File("ignite-config.xml"));
  }

  public OSHDB_Ignite(Ignite ignite) {
    this._ignite = ignite;
    this._ignite.active(true);
  }

  public OSHDB_Ignite(String igniteConfigFilePath) {
    this(new File(igniteConfigFilePath));
  }

  public OSHDB_Ignite(File igniteConfig) {
    Ignition.setClientMode(true);

    this._ignite = Ignition.start(igniteConfig.toString());
    this._ignite.active(true);
  }

  @Override
  public <X extends OSHDB_MapReducible> MapReducer<X> createMapReducer(Class<X> forClass) {
    MapReducer<X> mapReducer;
    switch (this.computeMode()) {
      case LocalPeek:
        mapReducer = new MapReducer_Ignite_LocalPeek<X>(this, forClass);
        break;
      case ScanQuery:
        mapReducer = new MapReducer_Ignite_ScanQuery<X>(this, forClass);
        break;
      case AffinityCall:
        mapReducer = new MapReducer_Ignite_AffinityCall<X>(this, forClass);
        break;
      default:
        throw new UnsupportedOperationException("Backend not implemented for this database option.");
    }
    return mapReducer;
  }

  public Ignite getIgnite() {
    return this._ignite;
  }

  public void close() {
    this._ignite.close();
  }

  public OSHDB_Ignite computeMode(ComputeMode computeMode) {
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
  public OSHDB_Ignite timeout(double seconds) {
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
  public OSHDB_Ignite timeoutInMilliseconds(long milliSeconds) {
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
