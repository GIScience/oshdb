package org.heigit.bigspatialdata.oshdb;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.BinaryOperator;

public class OSHDB_Ignite extends OSHDB {
  private final Ignite _ignite;

  public OSHDB_Ignite() throws SQLException, ClassNotFoundException {
    Ignition.setClientMode(true);

    /*IgniteConfiguration igniteCfg = new IgniteConfiguration();
    igniteCfg.setPeerClassLoadingEnabled(true);
    BinaryConfiguration binaryCfg = new BinaryConfiguration();
    binaryCfg.setCompactFooter(false);
    igniteCfg.setBinaryConfiguration(binaryCfg)
    this._ignite = Ignition.start(igniteCfg);*/

    this._ignite = Ignition.start("ignite-config.xml");
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
