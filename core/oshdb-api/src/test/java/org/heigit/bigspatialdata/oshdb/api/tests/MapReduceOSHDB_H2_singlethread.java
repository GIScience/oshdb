package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;

public class MapReduceOSHDB_H2_singlethread extends MapReduce {
  public MapReduceOSHDB_H2_singlethread() throws Exception {
    super(
        (new OSHDB_H2("./src/test/resources/test-data;ACCESS_MODE_DATA=r")).multithreading(false)
    );
  }
}
