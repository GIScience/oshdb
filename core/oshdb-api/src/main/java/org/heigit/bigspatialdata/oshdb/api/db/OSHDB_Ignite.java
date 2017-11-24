package org.heigit.bigspatialdata.oshdb.api.db;

import java.io.File;
import java.io.Serializable;
import java.sql.SQLException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_Ignite_LocalPeek;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_Ignite_ScanQuery;

public class OSHDB_Ignite extends OSHDB_Implementation implements AutoCloseable, Serializable {
  public enum ComputeMode {
    ScanQuery,
    LocalPeek
  }

  private final Ignite _ignite;
  private ComputeMode _computeMode = ComputeMode.ScanQuery;

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

  @Override
  public <X> MapReducer<X> createMapReducer(Class<?> forClass) {
    MapReducer<X> mapReducer;
    switch (this.computeMode()) {
      case ScanQuery:
        mapReducer = new MapReducer_Ignite_ScanQuery<X>(this, forClass);
        break;
      case LocalPeek:
        mapReducer = new MapReducer_Ignite_LocalPeek<X>(this, forClass);
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
}
