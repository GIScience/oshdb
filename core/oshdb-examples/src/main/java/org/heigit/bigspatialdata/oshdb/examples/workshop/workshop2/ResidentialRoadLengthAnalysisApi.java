package org.heigit.bigspatialdata.oshdb.examples.workshop.workshop2;

import java.util.Map;
import java.util.SortedMap;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapper.OSMContributionMapper;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.OSMType;

public class ResidentialRoadLengthAnalysisApi {
  public static void main(String[] args) throws Exception {
    // database
    //OSHDB_Ignite oshdb = new OSHDB_Ignite();
    //OSHDB oshdb = new OSHDB_H2("./karlsruhe-regbez").multithreading(true);
    //OSHDB oshdbKeytables = new OSHDB_H2("./karlsruhe-regbez");
    OSHDB oshdb = (new OSHDB_H2("./baden-wuerttemberg.oshdb")).multithreading(true);
    OSHDB oshdbKeytables = new OSHDB_H2("./keytables");

    // query
    SortedMap<OSHDBTimestamp, Number> result = OSMContributionMapper.using(oshdb).usingForTags(oshdbKeytables)
        //.boundingBox(new BoundingBox(8.6528,8.7294, 49.3683,49.4376))
        .boundingBox(new BoundingBox(7.19,10.91, 47.35,49.53))
        .timestamps(new OSHDBTimestamps(2008, 2017, 1, 12))
        .osmTypes(OSMType.NODE)
        .filterByTagValue("tourism", "information")
        .filterByTagValue("information", "guidepost")
        .filter(osmEntity -> true)
        .sumAggregateByTimestamp(
            x -> x.getContributionTypes().contains(ContributionType.DELETION) ? 1 : 0
            //snapshot -> Geo.lengthOf(snapshot.getGeometry())
        );

    // output
    for (Map.Entry<OSHDBTimestamp, Number> entry : result.entrySet())
      System.out.format("%s\t%.2f\n", entry.getKey().formatDate(), entry.getValue().doubleValue());

    //oshdb.close();
  }
}
