package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;

public class OSMEntitySnapshotView {
  public static MapReducer<OSMEntitySnapshot> on(OSHDBDatabase oshdb) {
    return oshdb.<OSMEntitySnapshot>createMapReducer(OSMEntitySnapshot.class);
  }
}
