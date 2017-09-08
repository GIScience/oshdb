package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;

public class OSMEntitySnapshotView extends MapperFactory {
  public static MapReducer<OSMEntitySnapshot> on(OSHDB oshdb) {
    return MapReducer.<OSMEntitySnapshot>using(oshdb, OSMEntitySnapshot.class);
  }
}
