package org.heigit.bigspatialdata.oshdb.api.db;

import java.io.File;
import java.io.Serializable;
import java.sql.SQLException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.heigit.bigspatialdata.oshdb.OSHDB;

public class OSHDB_Ignite extends OSHDB implements AutoCloseable, Serializable {
  public enum ComputeMode {
    ScanQuery,
    LocalPeek
  }

  private final Ignite _ignite;
  private ComputeMode _computeMode = ComputeMode.LocalPeek;

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
}
