package org.heigit.bigspatialdata.oshdb.examples.workshop.workshop2;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.objects.Timestamp;
import java.util.Map;
import java.util.SortedMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.heigit.bigspatialdata.oshdb.api.objects.Timestamps;
import org.heigit.bigspatialdata.oshdb.api.mapper.Mapper;

public class ResidentialRoadLengthAnalysisApi {
  public static void main(String[] args) throws Exception {
    // database
    OSHDB oshdb = new OSHDB_H2("./heidelberg--2017-05-29");

    // query
    SortedMap<Timestamp, Double> result = Mapper.using(oshdb)
            .boundingBox(new BoundingBox(8.6528,8.7294, 49.3683,49.4376))
            .timestamps(new Timestamps(2008, 2017, 1, 12))
            .filter(entity -> entity.hasTagValue(2, 0))
            .aggregate((timestamp, geometry, entity) -> new ImmutablePair<>(timestamp, Geo.distanceOf(geometry)));

    // output
    for (Map.Entry<Timestamp, Double> entry : result.entrySet()) System.out.format("%s\t%f\n", entry.getKey().date(), entry.getValue());
  }
}
