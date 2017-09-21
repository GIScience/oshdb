package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;

public class FlatMapReduceGroupedByIdOSHDB_H2_singlethread extends FlatMapReduceGroupedById {
  public FlatMapReduceGroupedByIdOSHDB_H2_singlethread() throws Exception {
    super(
        (new OSHDB_H2("./src/test/resources/test-data;ACCESS_MODE_DATA=r")).multithreading(false)
    );
  }
}
