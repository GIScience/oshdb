package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;

public class TestFlatMapReduceGroupedByIdOSHDB_H2_singlethread extends TestFlatMapReduceGroupedById {
  public TestFlatMapReduceGroupedByIdOSHDB_H2_singlethread() throws Exception {
    super(
        (new OSHDB_H2("./src/test/resources/test-data")).multithreading(false)
    );
  }
}
