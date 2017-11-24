package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Implementation;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;

public class OSMContributionView extends MapperFactory {
  public static MapReducer<OSMContribution> on(OSHDB_Implementation oshdb) {
    return MapReducer.<OSMContribution>using(oshdb, OSMContribution.class);
  }
}
