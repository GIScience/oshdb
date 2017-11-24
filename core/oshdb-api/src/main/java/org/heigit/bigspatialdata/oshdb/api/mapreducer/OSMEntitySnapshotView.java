package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Implementation;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;

public class OSMEntitySnapshotView extends MapperFactory {
  public static MapReducer<OSMEntitySnapshot> on(OSHDB_Implementation oshdb) {
    return oshdb.createMapReducer(OSMEntitySnapshot.class);
    //return MapReducer.<OSMEntitySnapshot>using(oshdb, OSMEntitySnapshot.class);
  }
}
