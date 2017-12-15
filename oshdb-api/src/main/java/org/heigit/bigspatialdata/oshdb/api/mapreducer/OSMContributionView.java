package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Implementation;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;

public class OSMContributionView {
  public static MapReducer<OSMContribution> on(OSHDB_Implementation oshdb) {
    return oshdb.<OSMContribution>createMapReducer(OSMContribution.class);
  }
}
