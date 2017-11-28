package org.heigit.bigspatialdata.oshdb.api.db;

import java.io.File;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.exceptions.OSHDBTimeoutException;

public class OSHDB_Ignite extends OSHDB implements AutoCloseable, Serializable {
  public enum ComputeMode {
    ScanQuery,
    LocalPeek
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

  public OSHDB_Ignite(File igniteXml) {
    Ignition.setClientMode(true);

    this._ignite = Ignition.start(igniteXml.toString());
    this._ignite.active(true);
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
