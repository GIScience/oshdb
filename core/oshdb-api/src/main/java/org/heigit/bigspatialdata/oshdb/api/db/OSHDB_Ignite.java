package org.heigit.bigspatialdata.oshdb.api.db;

import java.io.File;
import java.sql.SQLException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.heigit.bigspatialdata.oshdb.OSHDB;

public class OSHDB_Ignite extends OSHDB {

  private final Ignite _ignite;

  public OSHDB_Ignite() throws SQLException, ClassNotFoundException {
    this(new File("ignite-config.xml"));
  }

  public OSHDB_Ignite(Ignite ignite) {
    this._ignite = ignite;
    this._ignite.active(true);
  }

  public OSHDB_Ignite(File igniteXml) {
    Ignition.setClientMode(true);

    /*IgniteConfiguration igniteCfg = new IgniteConfiguration();
    igniteCfg.setPeerClassLoadingEnabled(true);
    BinaryConfiguration binaryCfg = new BinaryConfiguration();
    binaryCfg.setCompactFooter(false);
    igniteCfg.setBinaryConfiguration(binaryCfg)
    this._ignite = Ignition.start(igniteCfg);*/
    this._ignite = Ignition.start(igniteXml.toString());
    this._ignite.active(true);

    /*try (Ignite ignite = Ignition.start("ignite-config.xml")) {
      ignite.active(true);
      _ignite = ignite;
    }*/
  }

  public Ignite getIgnite() {
    return this._ignite;
  }

  public void close() {
    this._ignite.close();
  }

}
