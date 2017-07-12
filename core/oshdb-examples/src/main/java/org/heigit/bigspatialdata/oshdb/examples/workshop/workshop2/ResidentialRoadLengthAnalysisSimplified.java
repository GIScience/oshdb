package org.heigit.bigspatialdata.oshdb.examples.workshop.workshop2;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.OSMDB_H2;
import org.heigit.bigspatialdata.oshdb.utils.OSMTimeStamp;
import java.util.Map;
import java.util.SortedMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.heigit.bigspatialdata.oshdb.utils.OSMTimeStamps;

public class ResidentialRoadLengthAnalysisSimplified {
    public static void main(String[] args) throws Exception {
        // database
        OSHDB osmdb = new OSMDB_H2("./heidelberg--2017-05-29");
        
        // query
        SortedMap<OSMTimeStamp, Double> result = osmdb.createCellMapper(new BoundingBox(8.6528,8.7294, 49.3683,49.4376), new OSMTimeStamps(2008, 2017, 1, 12))
                .filter(entity -> entity.hasTagValue(2, 0))
                .aggregate((timestamp, geometry, entity) -> new ImmutablePair<>(timestamp, Geo.distanceOf(geometry)));
        
        // output
        for (Map.Entry<OSMTimeStamp, Double> entry : result.entrySet()) System.out.format("%s\t%f\n", entry.getKey().date(), entry.getValue());
    }
}
