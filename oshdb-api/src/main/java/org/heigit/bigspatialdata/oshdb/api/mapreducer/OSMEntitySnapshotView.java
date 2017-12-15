package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Implementation;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;

public class OSMEntitySnapshotView {
  public static MapReducer<OSMEntitySnapshot> on(OSHDB_Implementation oshdb) {
    return oshdb.<OSMEntitySnapshot>createMapReducer(OSMEntitySnapshot.class);
  }
}
