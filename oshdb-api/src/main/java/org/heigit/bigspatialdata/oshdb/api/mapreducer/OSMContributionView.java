package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;

public class OSMContributionView {
  public static MapReducer<OSMContribution> on(OSHDBDatabase oshdb) {
    return oshdb.<OSMContribution>createMapReducer(OSMContribution.class);
  }
}
