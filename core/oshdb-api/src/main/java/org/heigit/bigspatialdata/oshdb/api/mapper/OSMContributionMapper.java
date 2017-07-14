package org.heigit.bigspatialdata.oshdb.api.mapper;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;

public class OSMContributionMapper {
  public static Mapper<OSMContribution> using(OSHDB oshdb) {
    Mapper<OSMContribution> m = Mapper.<OSMContribution>using(oshdb);
    m._forClass = OSMContribution.class;
    return m;
  }
}
