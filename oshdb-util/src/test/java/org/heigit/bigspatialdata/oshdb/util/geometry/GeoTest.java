package org.heigit.bigspatialdata.oshdb.util.geometry;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class GeoTest {

    private static final double oneDegreeInMeters = 110000.;

    @Test
    public void TestIsWithinDistance() {

        Geometry geom1 = new GeometryFactory().createPoint(new Coordinate(48, 9)).buffer(1);
        Geometry geom2 = new GeometryFactory().createPoint(new Coordinate(45, 9)).buffer(1);
        double distanceInMeter = 1. * oneDegreeInMeters;

        assertTrue(Geo.isWithinDistance(geom1, geom2, distanceInMeter));
        assertTrue(!Geo.isWithinDistance(geom1, geom2, distanceInMeter/2.));
    }

}

