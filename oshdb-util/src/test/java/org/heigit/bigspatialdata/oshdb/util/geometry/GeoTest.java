package org.heigit.bigspatialdata.oshdb.util.geometry;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class GeoTest {

    @Test
    public void TestIsWithinDistance() {

        Geometry geom1 = new GeometryFactory().createPoint(new Coordinate(48, 9)).buffer(1);
        Geometry geom2 = new GeometryFactory().createPoint(new Coordinate(45, 9)).buffer(1);
        double distanceInMeter = 1. * Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR;

        assertTrue(Geo.isWithinDistance(geom1, geom2, distanceInMeter));
        assertTrue(!Geo.isWithinDistance(geom1, geom2, distanceInMeter/2.));
    }

    @Test
    public void TestConvertMetricDistanceToDegreeLongitude() {
        assertEquals(1., Geo.convertMetricDistanceToDegreeLongitude(0, Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR), 0.01 );
        assertEquals(1., Geo.convertMetricDistanceToDegreeLongitude(48, 74000.), 0.01);
    }

}