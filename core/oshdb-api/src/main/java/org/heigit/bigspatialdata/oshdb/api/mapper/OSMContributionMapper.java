package org.heigit.bigspatialdata.oshdb.api.mapper;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;

public class OSMContributionMapper implements MapperFactory {
  public static Mapper<OSMContribution> using(OSHDB oshdb) {
    return Mapper.<OSMContribution>using(oshdb, OSMContribution.class);
  }
}
