package org.heigit.ohsome.oshdb.util.geometry;

import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilderTest.checkAllMemberPermutations;
import static org.junit.jupiter.api.Assertions.*;

class OSHDBGeometryBuilderMultipolygonTest extends OSHDBGeometryTest {
    private final WKTReader wkt = new WKTReader();

    OSHDBGeometryBuilderMultipolygonTest(){
        super("./src/test/resources/relations/polygonShareNode.osm");
    }

    /**
     * expect Polygon with a hole
     * - POLYGON((0 0,0 2,2 2,4 2,4 0,0 0),(2 2,1 1,3 1,2 2))
     *
     * 2 1 --- 2 --- 5
     *   |     /\    |
     * 1 |   3 -- 4  |
     *   |           |
     * 0 0 --------- 6
     *   0  1  2  3  4
     *
     * @throws ParseException
     */
    @Test
    public void testPolygonShareNode() throws ParseException {
        var expect = wkt.read("POLYGON ((0 0, 0 2, 2 2, 4 2, 4 0, 0 0), (2 2, 1 1, 3 1, 2 2))");
        var members = testData.relations().get(0L).get(0).getMembers();
        var counter = new AtomicInteger();
        checkAllMemberPermutations(members.length, members, getMultipolygonSharedNodeCheck((permutation, geom) -> {
            if (!(geom.isValid() && geom instanceof Polygon && expect.equals(geom))) {
              System.out.printf("%2d - %s - %s%n", counter.getAndIncrement(), print(permutation), geom);
            }
            assertTrue(geom.isValid());
            assertEquals("Polygon", geom.getGeometryType());
            assertTrue(expect.equals(geom));
        }));
    }

    public static Consumer<OSMMember[]> getMultipolygonSharedNodeCheck(BiConsumer<OSMMember[], Geometry> tester) {
        var areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
        var timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
        return relMembers -> {
            var relation = OSM.relation(1, 1, timestamp.getEpochSecond(), 0, 0, null, relMembers);
            var geom = OSHDBGeometryBuilder.getGeometry(relation, timestamp, areaDecider);
            tester.accept(relMembers, geom);
        };
    }

    private static String print(OSMMember[] mems) {
        return Arrays.stream(mems).map(m -> "" + m.getId()).collect(Collectors.joining(""));
    }
}
