package org.heigit.bigspatialdata.oshdb.api;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.sql.SQLException;

public class OSMEntitySnapshotViewTestH2Multithread extends OSMEntitySnapshotViewTestH2Singlethread {
  public OSMEntitySnapshotViewTestH2Multithread() throws SQLException, ClassNotFoundException, IOException, ParseException {
    OSHDB_H2 oshdb = new OSHDB_H2("./src/test/resources/test-data");
    oshdb.multithreading(false);

    mapReducer = OSMEntitySnapshotView.on(oshdb);
  }
}
