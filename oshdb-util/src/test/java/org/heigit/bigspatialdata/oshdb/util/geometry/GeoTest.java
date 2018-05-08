package org.heigit.bigspatialdata.oshdb.util.geometry;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class GeoTest {

    private static final double ONE_DEGREE_IN_METERS_AT_EQUATOR = 110567.;

    @Test
    public void TestIsWithinDistance() {

        Geometry geom1 = new GeometryFactory().createPoint(new Coordinate(48, 9)).buffer(1);
        Geometry geom2 = new GeometryFactory().createPoint(new Coordinate(45, 9)).buffer(1);
        double distanceInMeter = 1. * ONE_DEGREE_IN_METERS_AT_EQUATOR;

        assertTrue(Geo.isWithinDistance(geom1, geom2, distanceInMeter));
        assertTrue(!Geo.isWithinDistance(geom1, geom2, distanceInMeter/2.));
    }

    @Test
    public void TestConvertMetricDistanceToDegree() {
        assertEquals(1., Geo.convertMetricDistanceToDegreeLongitude(0, ONE_DEGREE_IN_METERS_AT_EQUATOR), 0.001 );
        assertEquals(1., Geo.convertMetricDistanceToDegreeLongitude(48, 74000.), 0.001);
    }

}

