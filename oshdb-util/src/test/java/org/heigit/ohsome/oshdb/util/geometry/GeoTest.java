package org.heigit.ohsome.oshdb.util.geometry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link Geo} class.
 */
class GeoTest {
  private final GeometryFactory gf = new GeometryFactory();

  private Coordinate[] constructCoordinates(double...coordValues) {
    Coordinate[] coords = new Coordinate[coordValues.length / 2];
    for (int i = 0;  i < coordValues.length / 2; i++) {
      coords[i] = new Coordinate(coordValues[i * 2], coordValues[i * 2 + 1]);
    }
    return coords;
  }

  private LinearRing constructRing(double...coordValues) {
    return gf.createLinearRing(constructCoordinates(coordValues));
  }

  // Geo.areaOf

  @Test
  void testAreaPolygon() {
    LinearRing outer = constructRing(
        0, 0,
        0, 1,
        1, 1,
        1, 0,
        0, 0
    );
    LinearRing inner = constructRing(
        0.5, 0.5,
        0.5, 0.6,
        0.6, 0.6,
        0.6, 0.5,
        0.5, 0.5
    );
    // compared with result from geojson.io, allow 5% error to compensate for different
    // geometry calculation parameters (earth "radius", etc.)
    Geometry poly = gf.createPolygon(outer);
    assertEquals(1.0, 12391399902.0 / Geo.areaOf(poly), 0.05);
    // check that poly with whole is actually ~1% smaller in size.
    Geometry polyWithInner = gf.createPolygon(outer, new LinearRing[] { inner });
    assertEquals(0.99, Geo.areaOf(polyWithInner) / Geo.areaOf(poly), 0.0001);
  }

  @Test
  void testAreaMultiPolygon() {
    Polygon poly1 = gf.createPolygon(constructRing(
        0, 0,
        0, 1,
        1, 1,
        1, 0,
        0, 0
    ));
    Polygon poly2 = gf.createPolygon(constructRing(
        2, 0,
        2, 1,
        3, 1,
        3, 0,
        2, 0
    ));
    // check that multipolygon is 200% larger in size than single poly.
    MultiPolygon mpoly = gf.createMultiPolygon(new Polygon[] { poly1, poly2 });
    assertEquals(2.0, Geo.areaOf(mpoly) / Geo.areaOf(poly1), 0.0001);
  }

  @Test
  void testAreaGeometryCollection() {
    Polygon poly1 = gf.createPolygon(constructRing(
        0, 0,
        0, 1,
        1, 1,
        1, 0,
        0, 0
    ));
    Polygon poly2 = gf.createPolygon(constructRing(
        2, 0,
        2, 1,
        3, 1,
        3, 0,
        2, 0
    ));
    // check that collection is 200% larger in size than single poly.
    GeometryCollection gcoll = gf.createGeometryCollection(new Geometry[]{ poly1, poly2 });
    assertEquals(2.0, Geo.areaOf(gcoll) / Geo.areaOf(poly1), 0.0001);
    // check that collections with non-polygon members are ignored.
    GeometryCollection gcoll2 = gf.createGeometryCollection(new Geometry[]{
        poly1,
        gf.createPoint(new Coordinate(0, 0)),
        poly2.getExteriorRing()
    });
    assertEquals(1.0, Geo.areaOf(gcoll2) / Geo.areaOf(poly1), 0.0001);
  }

  @Test
  void testAreaOther() {
    // other geometry types: area should be returned as zero
    // point
    assertEquals(0.0, Geo.areaOf(gf.createPoint(new Coordinate(0, 0))), 1E-22);
    // multi point
    assertEquals(0.0, Geo.areaOf(gf.createMultiPoint(new Point[]{
        gf.createPoint(new Coordinate(0, 0)),
        gf.createPoint(new Coordinate(1, 1))
    })), 1E-22);
    // linestring
    assertEquals(0.0, Geo.areaOf(gf.createLineString(
        constructCoordinates(0, 0, 1, 1, 0, 1, 0, 0)
    )), 1E-22);
    // multi linestring
    assertEquals(0.0, Geo.areaOf(gf.createMultiLineString(new LineString[]{
        gf.createLineString(constructCoordinates(0, 0, 1, 1, 0, 1, 0, 0)),
        gf.createLineString(constructCoordinates(1, 1, 2, 2, 1, 2, 1, 1))
    })), 1E-22);
  }

  @Test
  void testAreaRealFeatures() {
    final double relativeDelta = 1E-5; // max 0.001 % error
    // use https://www.openstreetmap.org/way/25316219 state 2020-10-29
    double expectedResult = 5797.767; // calculated with QGIS
    Polygon polygon = gf.createPolygon(featureSmall);
    assertEquals(1.0, Geo.areaOf(polygon) / expectedResult, relativeDelta);

    // use https://www.openstreetmap.org/relation/7579947 state 2020-10-29
    expectedResult = 2386876807.747; // calculated with QGIS
    polygon = gf.createPolygon(featureLarge);
    assertEquals(1.0, Geo.areaOf(polygon) / expectedResult, relativeDelta);

    // use https://www.openstreetmap.org/relation/2793215 state 2020-10-29
    expectedResult = 2033079184.522; // calculated with QGIS
    polygon = gf.createPolygon(featureEquator);
    assertEquals(1.0, Geo.areaOf(polygon) / expectedResult, relativeDelta);

    // use https://www.openstreetmap.org/way/226763404 state 2020-10-29
    expectedResult = 410425.251; // calculated with QGIS
    polygon = gf.createPolygon(featurePole);
    assertEquals(1.0, Geo.areaOf(polygon) / expectedResult, relativeDelta);
  }

  @Test
  void testAreaNotNegative() {
    Polygon poly = gf.createPolygon(constructRing(
        0, 0,
        0, 1,
        1, 1,
        1, 0,
        0, 0
    ), new LinearRing[] { constructRing(
        0, 0,
        0, 3,
        3, 3,
        3, 0,
        0, 0
    )});
    // check that area is not negative
    assertFalse(Geo.areaOf(poly) < 0);
  }


  // Geo.lengthOf

  @Test
  void testLengthLineString() {
    LineString line = gf.createLineString(constructCoordinates(
        0, 0,
        1, 1
    ));
    // compared with result from geojson.io, allow 5% error to compensate for different
    // geometry calculation parameters (earth "radius", etc.)
    assertEquals(1.0, 157425.5 / Geo.lengthOf(line), 0.05);
  }

  @Test
  void testLengthMultiLineString() {
    LineString line1 = gf.createLineString(constructCoordinates(
        0, 0,
        1, 1
    ));
    LineString line2 = gf.createLineString(constructCoordinates(
        1, 1,
        2, 0
    ));
    // check that multilinestring is 200% larger in size than single line.
    MultiLineString mline = gf.createMultiLineString(new LineString[] { line1, line2 });
    assertEquals(2.0, Geo.lengthOf(mline) / Geo.lengthOf(line1), 0.0001);
  }

  @Test
  void testLengthGeometryCollection() {
    LineString line1 = gf.createLineString(constructCoordinates(
        0, 0,
        1, 1
    ));
    LineString line2 = gf.createLineString(constructCoordinates(
        1, 1,
        2, 0
    ));
    Polygon poly1 = gf.createPolygon(constructRing(
        0, 0,
        0, 1,
        1, 1,
        1, 0,
        0, 0
    ));
    // check that collection is 200% larger in size than single poly.
    GeometryCollection gcoll = gf.createGeometryCollection(new Geometry[]{ line1, line2 });
    assertEquals(2.0, Geo.lengthOf(gcoll) / Geo.lengthOf(line1), 0.0001);
    // check that collections with non-polygon members are ignored.
    GeometryCollection gcoll2 = gf.createGeometryCollection(new Geometry[]{
        line1,
        gf.createPoint(new Coordinate(0, 0)),
        poly1
    });
    assertEquals(1.0, Geo.lengthOf(gcoll2) / Geo.lengthOf(line1), 0.0001);
  }

  @Test
  void testLengthOther() {
    // other geometry types: area should be returned as zero
    // point
    assertEquals(0.0, Geo.lengthOf(gf.createPoint(new Coordinate(0, 0))), 1E-22);
    // multi point
    assertEquals(0.0, Geo.lengthOf(gf.createMultiPoint(new Point[]{
        gf.createPoint(new Coordinate(0, 0)),
        gf.createPoint(new Coordinate(1, 1))
    })), 1E-22);
    // polygon
    assertEquals(0.0, Geo.lengthOf(gf.createPolygon(
        constructRing(0, 0, 1, 1, 0, 1, 0, 0)
    )), 1E-22);
    // multi polygon
    assertEquals(0.0, Geo.lengthOf(gf.createMultiPolygon(new Polygon[]{
        gf.createPolygon(constructRing(0, 0, 1, 1, 0, 1, 0, 0)),
        gf.createPolygon(constructRing(1, 1, 2, 2, 1, 2, 1, 1))
    })), 1E-22);
  }

  @Test
  void testLengthRealFeatures() {
    final double relativeDelta = 1E-3; // max 0.1 % error
    // use https://www.openstreetmap.org/way/25316219 state 2020-10-29
    double expectedResult = 330.201; // calculated with QGIS
    LineString line = gf.createLineString(featureSmall);
    assertEquals(1.0, Geo.lengthOf(line) / expectedResult, relativeDelta);

    // use https://www.openstreetmap.org/relation/7579947 state 2020-10-29
    expectedResult = 259588.182; // calculated with QGIS
    line = gf.createLineString(featureLarge);
    assertEquals(1.0, Geo.lengthOf(line) / expectedResult, relativeDelta);

    // use https://www.openstreetmap.org/relation/2793215 state 2020-10-29
    expectedResult = 160194.395; // calculated with QGIS
    line = gf.createLineString(featureEquator);
    assertEquals(1.0, Geo.lengthOf(line) / expectedResult, relativeDelta);

    // use https://www.openstreetmap.org/way/226763404 state 2020-10-29
    expectedResult = 2525.821; // calculated with QGIS
    line = gf.createLineString(featurePole);
    assertEquals(1.0, Geo.lengthOf(line) / expectedResult, relativeDelta);
  }

  // real world test geometries

  private static final Coordinate[] featureSmall = {
      new Coordinate(8.6707583, 49.4160821),
      new Coordinate(8.6707699, 49.4160822),
      new Coordinate(8.6708742, 49.4160827),
      new Coordinate(8.6710011, 49.4160836),
      new Coordinate(8.6710260, 49.4160838),
      new Coordinate(8.6710753, 49.4160498),
      new Coordinate(8.6711725, 49.4159829),
      new Coordinate(8.6711738, 49.4159529),
      new Coordinate(8.6711782, 49.4158550),
      new Coordinate(8.6712243, 49.4158558),
      new Coordinate(8.6712246, 49.4157406),
      new Coordinate(8.6712255, 49.4155852),
      new Coordinate(8.6711074, 49.4155117),
      new Coordinate(8.6711100, 49.4154742),
      new Coordinate(8.6710811, 49.4154520),
      new Coordinate(8.6710856, 49.4153545),
      new Coordinate(8.6710943, 49.4153034),
      new Coordinate(8.6711071, 49.4152463),
      new Coordinate(8.6710668, 49.4152173),
      new Coordinate(8.6710400, 49.4151994),
      new Coordinate(8.6708966, 49.4151038),
      new Coordinate(8.6707620, 49.4151056),
      new Coordinate(8.6706808, 49.4151569),
      new Coordinate(8.6706648, 49.4151651),
      new Coordinate(8.6705213, 49.4151652),
      new Coordinate(8.6704981, 49.4151652),
      new Coordinate(8.6704477, 49.4151342),
      new Coordinate(8.6703678, 49.4151333),
      new Coordinate(8.6702422, 49.4152155),
      new Coordinate(8.6702231, 49.4152280),
      new Coordinate(8.6702239, 49.4152995),
      new Coordinate(8.6702244, 49.4153317),
      new Coordinate(8.6702457, 49.4153428),
      new Coordinate(8.6702466, 49.4153716),
      new Coordinate(8.6702534, 49.4153978),
      new Coordinate(8.6702757, 49.4154131),
      new Coordinate(8.6703554, 49.4154569),
      new Coordinate(8.6703239, 49.4158876),
      new Coordinate(8.6706476, 49.4158871),
      new Coordinate(8.6706478, 49.4159317),
      new Coordinate(8.6706480, 49.4160138),
      new Coordinate(8.6707583, 49.4160821)};
  private static final Coordinate[] featureLarge = {
      new Coordinate(8.7020961, 49.3169261),
      new Coordinate(8.7033451, 49.3174857),
      new Coordinate(8.7032491, 49.3206511),
      new Coordinate(8.7037153, 49.3213267),
      new Coordinate(8.7050968, 49.3216004),
      new Coordinate(8.7051162, 49.3232808),
      new Coordinate(8.7030032, 49.3237637),
      new Coordinate(8.7015813, 49.3230411),
      new Coordinate(8.7008297, 49.3234224),
      new Coordinate(8.6996626, 49.3237223),
      new Coordinate(8.6997570, 49.3239811),
      new Coordinate(8.7009544, 49.3239386),
      new Coordinate(8.7014064, 49.3244137),
      new Coordinate(8.7008909, 49.3253153),
      new Coordinate(8.7000841, 49.3254627),
      new Coordinate(8.7004405, 49.3304670),
      new Coordinate(8.7009839, 49.3372122),
      new Coordinate(8.7004066, 49.3377982),
      new Coordinate(8.7029296, 49.3385560),
      new Coordinate(8.7030752, 49.3410196),
      new Coordinate(8.7026605, 49.3428747),
      new Coordinate(8.7000925, 49.3432516),
      new Coordinate(8.6993874, 49.3448283),
      new Coordinate(8.7020469, 49.3457213),
      new Coordinate(8.7019488, 49.3462903),
      new Coordinate(8.7017900, 49.3480361),
      new Coordinate(8.7024531, 49.3480782),
      new Coordinate(8.7035101, 49.3505834),
      new Coordinate(8.7039662, 49.3532004),
      new Coordinate(8.7048822, 49.3534161),
      new Coordinate(8.7057323, 49.3545139),
      new Coordinate(8.7049386, 49.3556118),
      new Coordinate(8.7097114, 49.3566798),
      new Coordinate(8.7084636, 49.3576011),
      new Coordinate(8.7058693, 49.3580144),
      new Coordinate(8.7038306, 49.3583243),
      new Coordinate(8.7054905, 49.3605078),
      new Coordinate(8.7055600, 49.3631715),
      new Coordinate(8.7063341, 49.3654818),
      new Coordinate(8.7078823, 49.3659784),
      new Coordinate(8.7077830, 49.3695440),
      new Coordinate(8.7067498, 49.3703485),
      new Coordinate(8.7078962, 49.3738306),
      new Coordinate(8.7080296, 49.3756108),
      new Coordinate(8.7086674, 49.3774396),
      new Coordinate(8.7068877, 49.3807357),
      new Coordinate(8.7055219, 49.3809706),
      new Coordinate(8.7023011, 49.3811788),
      new Coordinate(8.6988824, 49.3784499),
      new Coordinate(8.6973745, 49.3780736),
      new Coordinate(8.6961301, 49.3777036),
      new Coordinate(8.6950352, 49.3782920),
      new Coordinate(8.6945836, 49.3816172),
      new Coordinate(8.6935630, 49.3844227),
      new Coordinate(8.6932536, 49.3870720),
      new Coordinate(8.6936733, 49.3895233),
      new Coordinate(8.6926227, 49.3944986),
      new Coordinate(8.6908670, 49.3955847),
      new Coordinate(8.6916858, 49.3991147),
      new Coordinate(8.6928730, 49.3997319),
      new Coordinate(8.6932092, 49.4018808),
      new Coordinate(8.6935049, 49.4042166),
      new Coordinate(8.6946487, 49.4069942),
      new Coordinate(8.6972232, 49.4081313),
      new Coordinate(8.7057309, 49.4091744),
      new Coordinate(8.7103539, 49.4099639),
      new Coordinate(8.7152677, 49.4126583),
      new Coordinate(8.7137871, 49.4138449),
      new Coordinate(8.7017709, 49.4149059),
      new Coordinate(8.6942821, 49.4164135),
      new Coordinate(8.6910206, 49.4182003),
      new Coordinate(8.6904841, 49.4219551),
      new Coordinate(8.6907523, 49.4272798),
      new Coordinate(8.6876535, 49.4314637),
      new Coordinate(8.6825877, 49.4351016),
      new Coordinate(8.6767298, 49.4434873),
      new Coordinate(8.6789184, 49.4469752),
      new Coordinate(8.6786284, 49.4522660),
      new Coordinate(8.6763883, 49.4553177),
      new Coordinate(8.6774639, 49.4569585),
      new Coordinate(8.6771160, 49.4596547),
      new Coordinate(8.6735969, 49.4624999),
      new Coordinate(8.6690550, 49.4688191),
      new Coordinate(8.6667160, 49.4757908),
      new Coordinate(8.6661224, 49.4799363),
      new Coordinate(8.6681752, 49.4853405),
      new Coordinate(8.6688566, 49.4907790),
      new Coordinate(8.6668610, 49.4954342),
      new Coordinate(8.6666947, 49.5015777),
      new Coordinate(8.6705999, 49.5061204),
      new Coordinate(8.6729603, 49.5180881),
      new Coordinate(8.6693125, 49.5212363),
      new Coordinate(8.6693124, 49.5253732),
      new Coordinate(8.6672740, 49.5331587),
      new Coordinate(8.6698274, 49.5394250),
      new Coordinate(8.6682343, 49.5407500),
      new Coordinate(8.6707234, 49.5430614),
      new Coordinate(8.6745481, 49.5480434),
      new Coordinate(8.6763721, 49.5524704),
      new Coordinate(8.6768603, 49.5572612),
      new Coordinate(8.6735344, 49.5601841),
      new Coordinate(8.6685185, 49.5631604),
      new Coordinate(8.6646294, 49.5683814),
      new Coordinate(8.6632399, 49.5746689),
      new Coordinate(8.6626768, 49.5776626),
      new Coordinate(8.6599946, 49.5806957),
      new Coordinate(8.6589012, 49.5839503),
      new Coordinate(8.6576772, 49.5954826),
      new Coordinate(8.6543779, 49.6020168),
      new Coordinate(8.6549520, 49.6075119),
      new Coordinate(8.6552684, 49.6109302),
      new Coordinate(8.6509180, 49.6174113),
      new Coordinate(8.6481766, 49.6204466),
      new Coordinate(8.6450815, 49.6228606),
      new Coordinate(8.6417877, 49.6267388),
      new Coordinate(8.6414337, 49.6277186),
      new Coordinate(8.6421847, 49.6312281),
      new Coordinate(8.6450224, 49.6333383),
      new Coordinate(8.6483697, 49.6359232),
      new Coordinate(8.6497571, 49.6370107),
      new Coordinate(8.6498718, 49.6410509),
      new Coordinate(8.6416750, 49.6485538),
      new Coordinate(8.6377621, 49.6668209),
      new Coordinate(8.6310694, 49.6796504),
      new Coordinate(8.6254904, 49.6882302),
      new Coordinate(8.6280225, 49.7058293),
      new Coordinate(8.6254546, 49.7190362),
      new Coordinate(8.6252759, 49.7198708),
      new Coordinate(8.6200941, 49.7248288),
      new Coordinate(8.6177657, 49.7270564),
      new Coordinate(8.6342023, 49.7458894),
      new Coordinate(8.6466263, 49.7587854),
      new Coordinate(8.6542223, 49.7675730),
      new Coordinate(8.6511967, 49.7840322),
      new Coordinate(8.6560569, 49.7896361),
      new Coordinate(8.6574624, 49.7980788),
      new Coordinate(8.6590502, 49.8035633),
      new Coordinate(8.6679766, 49.8169395),
      new Coordinate(8.6695645, 49.8259381),
      new Coordinate(8.6651972, 49.8344265),
      new Coordinate(8.6506818, 49.8378966),
      new Coordinate(8.6651871, 49.8508483),
      new Coordinate(8.6730407, 49.8527853),
      new Coordinate(8.6947987, 49.8642945),
      new Coordinate(8.7057851, 49.8640732),
      new Coordinate(8.7221358, 49.8644882),
      new Coordinate(8.7495737, 49.8468732),
      new Coordinate(8.7710808, 49.8250285),
      new Coordinate(8.7824040, 49.8245357),
      new Coordinate(8.8026171, 49.8310415),
      new Coordinate(8.9097402, 49.8413880),
      new Coordinate(8.9235096, 49.8440781),
      new Coordinate(8.9679699, 49.8453511),
      new Coordinate(8.9600306, 49.8577475),
      new Coordinate(8.9678841, 49.8680109),
      new Coordinate(8.9702874, 49.8789911),
      new Coordinate(8.9649659, 49.8843282),
      new Coordinate(8.9729975, 49.8897407),
      new Coordinate(9.0104990, 49.8804844),
      new Coordinate(9.0357333, 49.8852960),
      new Coordinate(9.0453892, 49.8865126),
      new Coordinate(9.0562468, 49.8852407),
      new Coordinate(9.0692501, 49.8943924),
      new Coordinate(9.0872317, 49.9005017),
      new Coordinate(9.0994625, 49.9039846),
      new Coordinate(9.1170149, 49.8907982),
      new Coordinate(9.1434937, 49.8631146),
      new Coordinate(9.1319065, 49.8551744),
      new Coordinate(9.1025954, 49.8394285),
      new Coordinate(9.0989905, 49.8367714),
      new Coordinate(9.1001063, 49.8243419),
      new Coordinate(9.1222077, 49.8172814),
      new Coordinate(9.1308337, 49.7970911),
      new Coordinate(9.1479569, 49.7856490),
      new Coordinate(9.1487723, 49.7734835),
      new Coordinate(9.1601448, 49.7666374),
      new Coordinate(9.1548663, 49.7602338),
      new Coordinate(9.1738777, 49.7533858),
      new Coordinate(9.1738348, 49.7479788),
      new Coordinate(9.1616898, 49.7466200),
      new Coordinate(9.1606598, 49.7384388),
      new Coordinate(9.1779976, 49.7357484),
      new Coordinate(9.1855507, 49.7289524),
      new Coordinate(9.1978674, 49.7239311),
      new Coordinate(9.2005282, 49.7163565),
      new Coordinate(9.2129307, 49.7052839),
      new Coordinate(9.2167823, 49.6985336),
      new Coordinate(9.2259233, 49.6971179),
      new Coordinate(9.2338720, 49.6994065),
      new Coordinate(9.2372530, 49.7028637),
      new Coordinate(9.2539899, 49.6958410),
      new Coordinate(9.2714565, 49.6959798),
      new Coordinate(9.2779823, 49.6962174),
      new Coordinate(9.2805975, 49.7009485),
      new Coordinate(9.2886655, 49.7038629),
      new Coordinate(9.3143718, 49.7070824),
      new Coordinate(9.3254440, 49.7011706),
      new Coordinate(9.3394344, 49.6927596),
      new Coordinate(9.3460863, 49.6894559),
      new Coordinate(9.3533390, 49.6863186),
      new Coordinate(9.3594329, 49.6780161),
      new Coordinate(9.3695181, 49.6744337),
      new Coordinate(9.3729513, 49.6717675),
      new Coordinate(9.3777149, 49.6695734),
      new Coordinate(9.3896024, 49.6691567),
      new Coordinate(9.4060390, 49.6640181),
      new Coordinate(9.4104593, 49.6624348),
      new Coordinate(9.4177978, 49.6631015),
      new Coordinate(9.4249389, 49.6611847),
      new Coordinate(9.4289301, 49.6553229),
      new Coordinate(9.4309042, 49.6468207),
      new Coordinate(9.4338224, 49.6411517),
      new Coordinate(9.4409292, 49.6315907),
      new Coordinate(9.4455812, 49.6251137),
      new Coordinate(9.4481819, 49.6168564),
      new Coordinate(9.4499414, 49.6136866),
      new Coordinate(9.4488857, 49.6117679),
      new Coordinate(9.4357364, 49.6100160),
      new Coordinate(9.4149654, 49.5976118),
      new Coordinate(9.3995159, 49.5955812),
      new Coordinate(9.3840664, 49.5930498),
      new Coordinate(9.3789594, 49.5904905),
      new Coordinate(9.3711488, 49.5909912),
      new Coordinate(9.3690460, 49.5929664),
      new Coordinate(9.3561285, 49.5895168),
      new Coordinate(9.3489187, 49.5915476),
      new Coordinate(9.3446701, 49.5902957),
      new Coordinate(9.3447559, 49.5821717),
      new Coordinate(9.3417518, 49.5786935),
      new Coordinate(9.3471162, 49.5726826),
      new Coordinate(9.3609779, 49.5671720),
      new Coordinate(9.3520515, 49.5617721),
      new Coordinate(9.3158310, 49.5425336),
      new Coordinate(9.3129127, 49.5307807),
      new Coordinate(9.3052781, 49.5261849),
      new Coordinate(9.2991369, 49.5179663),
      new Coordinate(9.3054025, 49.5077402),
      new Coordinate(9.2749756, 49.5018878),
      new Coordinate(9.2580240, 49.4924110),
      new Coordinate(9.2519729, 49.4668982),
      new Coordinate(9.2485762, 49.4595918),
      new Coordinate(9.2421024, 49.4576378),
      new Coordinate(9.2339914, 49.4523095),
      new Coordinate(9.2421453, 49.4453623),
      new Coordinate(9.2418020, 49.4377443),
      new Coordinate(9.2349355, 49.4293716),
      new Coordinate(9.2220180, 49.4190433),
      new Coordinate(9.2148254, 49.4177591),
      new Coordinate(9.2062423, 49.4065068),
      new Coordinate(9.2060278, 49.4031836),
      new Coordinate(9.2004488, 49.3981287),
      new Coordinate(9.1736095, 49.3852796),
      new Coordinate(9.1645544, 49.3796081),
      new Coordinate(9.1566579, 49.3758082),
      new Coordinate(9.1466780, 49.3761714),
      new Coordinate(9.1461630, 49.3727903),
      new Coordinate(9.1452854, 49.3616675),
      new Coordinate(9.1427534, 49.3552386),
      new Coordinate(9.1393879, 49.3541376),
      new Coordinate(9.1392291, 49.3540856),
      new Coordinate(9.1331833, 49.3521077),
      new Coordinate(9.0970619, 49.3657738),
      new Coordinate(9.0913971, 49.3786281),
      new Coordinate(9.0440185, 49.3743810),
      new Coordinate(9.0258224, 49.4094646),
      new Coordinate(8.9959533, 49.3895795),
      new Coordinate(8.9681442, 49.3833218),
      new Coordinate(8.9375885, 49.3611902),
      new Coordinate(8.9190490, 49.3455356),
      new Coordinate(8.8781936, 49.3605194),
      new Coordinate(8.8163955, 49.3354692),
      new Coordinate(8.8098724, 49.3678978),
      new Coordinate(8.7741668, 49.3605194),
      new Coordinate(8.7652405, 49.3455356),
      new Coordinate(8.7532242, 49.3502325),
      new Coordinate(8.7453277, 49.3446409),
      new Coordinate(8.7351568, 49.3435504),
      new Coordinate(8.7317370, 49.3411386),
      new Coordinate(8.7280762, 49.3400526),
      new Coordinate(8.7308039, 49.3366297),
      new Coordinate(8.7305807, 49.3350909),
      new Coordinate(8.7248308, 49.3333416),
      new Coordinate(8.7261983, 49.3307640),
      new Coordinate(8.7284064, 49.3245465),
      new Coordinate(8.7255820, 49.3234142),
      new Coordinate(8.7238037, 49.3225010),
      new Coordinate(8.7212372, 49.3217946),
      new Coordinate(8.7195412, 49.3206499),
      new Coordinate(8.7175668, 49.3201932),
      new Coordinate(8.7154055, 49.3200044),
      new Coordinate(8.7113446, 49.3199922),
      new Coordinate(8.7109375, 49.3192249),
      new Coordinate(8.7159540, 49.3176401),
      new Coordinate(8.7198446, 49.3148263),
      new Coordinate(8.7179044, 49.3139187),
      new Coordinate(8.7167662, 49.3138545),
      new Coordinate(8.7148335, 49.3151133),
      new Coordinate(8.7131946, 49.3147197),
      new Coordinate(8.7122445, 49.3157164),
      new Coordinate(8.7107712, 49.3157631),
      new Coordinate(8.7092226, 49.3141280),
      new Coordinate(8.7082924, 49.3145110),
      new Coordinate(8.7081466, 49.3154297),
      new Coordinate(8.7073658, 49.3157396),
      new Coordinate(8.7057928, 49.3147749),
      new Coordinate(8.7061587, 49.3125413),
      new Coordinate(8.7035670, 49.3148798),
      new Coordinate(8.7018789, 49.3161465),
      new Coordinate(8.6998206, 49.3154890),
      new Coordinate(8.6990777, 49.3157904),
      new Coordinate(8.6980700, 49.3157501),
      new Coordinate(8.6979216, 49.3164647),
      new Coordinate(8.6987830, 49.3165302),
      new Coordinate(8.6995761, 49.3171086),
      new Coordinate(8.7020961, 49.3169261)};
  private static final Coordinate[] featureEquator = {
      new Coordinate(5.6340486, -1.6732196),
      new Coordinate(5.6673085, -1.6717958),
      new Coordinate(5.6835781, -1.6690746),
      new Coordinate(5.6995676, -1.6650206),
      new Coordinate(5.7151677, -1.6596613),
      new Coordinate(5.7302721, -1.6530334),
      new Coordinate(5.7447777, -1.6451821),
      new Coordinate(5.7585856, -1.6361610),
      new Coordinate(5.7716014, -1.6260316),
      new Coordinate(5.7887849, -1.6096500),
      new Coordinate(5.8020065, -1.5939469),
      new Coordinate(5.8155310, -1.5735695),
      new Coordinate(5.8201848, -1.5677480),
      new Coordinate(5.8392792, -1.5289465),
      new Coordinate(5.8430300, -1.5120000),
      new Coordinate(5.8444764, -1.5080000),
      new Coordinate(5.8458277, -1.5040000),
      new Coordinate(5.8470858, -1.5000000),
      new Coordinate(5.8488024, -1.4940000),
      new Coordinate(5.8498353, -1.4900000),
      new Coordinate(5.8512211, -1.4840000),
      new Coordinate(5.8520377, -1.4800000),
      new Coordinate(5.8531042, -1.4740000),
      new Coordinate(5.8537111, -1.4700000),
      new Coordinate(5.8544670, -1.4640000),
      new Coordinate(5.8548690, -1.4600000),
      new Coordinate(5.8553202, -1.4540000),
      new Coordinate(5.8555203, -1.4500000),
      new Coordinate(5.8556701, -1.4440000),
      new Coordinate(5.8560454, -1.4305041),
      new Coordinate(5.8576424, -1.4075566),
      new Coordinate(5.8565883, -1.3845778),
      new Coordinate(5.8541623, -1.3680156),
      new Coordinate(5.8500323, -1.3505566),
      new Coordinate(5.8465171, -1.3394452),
      new Coordinate(5.8375732, -1.3179314),
      new Coordinate(5.8261835, -1.2976057),
      new Coordinate(5.8125029, -1.2787449),
      new Coordinate(5.8005756, -1.2654341),
      new Coordinate(5.7875131, -1.2532493),
      new Coordinate(5.7734136, -1.2422803),
      new Coordinate(5.7583902, -1.2326151),
      new Coordinate(5.7360260, -1.2214349),
      new Coordinate(5.7155753, -1.2138930),
      new Coordinate(5.6944243, -1.2086244),
      new Coordinate(5.6722086, -1.2060000),
      new Coordinate(5.6669308, -1.2050000),
      new Coordinate(5.6606524, -1.2040000),
      new Coordinate(5.6524582, -1.2030000),
      new Coordinate(5.6348489, -1.2020000),
      new Coordinate(5.6286923, -1.2020000),
      new Coordinate(5.6110831, -1.2030000),
      new Coordinate(5.6028889, -1.2040000),
      new Coordinate(5.5966104, -1.2050000),
      new Coordinate(5.5913327, -1.2060000),
      new Coordinate(5.5866984, -1.2070000),
      new Coordinate(5.5825235, -1.2080000),
      new Coordinate(5.5786983, -1.2090000),
      new Coordinate(5.5751512, -1.2100000),
      new Coordinate(5.5718323, -1.2110000),
      new Coordinate(5.5657429, -1.2130000),
      new Coordinate(5.5629233, -1.2140000),
      new Coordinate(5.5576482, -1.2160000),
      new Coordinate(5.5527779, -1.2180000),
      new Coordinate(5.5504713, -1.2190000),
      new Coordinate(5.5460806, -1.2210000),
      new Coordinate(5.5419504, -1.2230000),
      new Coordinate(5.5380433, -1.2250000),
      new Coordinate(5.5339130, -1.2270000),
      new Coordinate(5.5300084, -1.2290000),
      new Coordinate(5.5263023, -1.2310000),
      new Coordinate(5.5227727, -1.2330000),
      new Coordinate(5.5177707, -1.2360000),
      new Coordinate(5.5099463, -1.2410000),
      new Coordinate(5.5069108, -1.2430000),
      new Coordinate(5.5025712, -1.2460000),
      new Coordinate(5.4984631, -1.2490000),
      new Coordinate(5.4945637, -1.2520000),
      new Coordinate(5.4896563, -1.2560000),
      new Coordinate(5.4850490, -1.2600000),
      new Coordinate(5.4807116, -1.2640000),
      new Coordinate(5.4776206, -1.2670000),
      new Coordinate(5.4736985, -1.2710000),
      new Coordinate(5.4699876, -1.2750000),
      new Coordinate(5.4664724, -1.2790000),
      new Coordinate(5.4631396, -1.2830000),
      new Coordinate(5.4599777, -1.2870000),
      new Coordinate(5.4569768, -1.2910000),
      new Coordinate(5.4541279, -1.2950000),
      new Coordinate(5.4514233, -1.2990000),
      new Coordinate(5.4488562, -1.3030000),
      new Coordinate(5.4464203, -1.3070000),
      new Coordinate(5.4435516, -1.3120000),
      new Coordinate(5.4413918, -1.3160000),
      new Coordinate(5.4393473, -1.3200000),
      new Coordinate(5.4374142, -1.3240000),
      new Coordinate(5.4347159, -1.3300000),
      new Coordinate(5.4330467, -1.3340000),
      new Coordinate(5.4307305, -1.3400000),
      new Coordinate(5.4293078, -1.3440000),
      new Coordinate(5.4273505, -1.3500000),
      new Coordinate(5.4261607, -1.3540000),
      new Coordinate(5.4250609, -1.3580000),
      new Coordinate(5.4235429, -1.3640000),
      new Coordinate(5.4221676, -1.3700000),
      new Coordinate(5.4209300, -1.3760000),
      new Coordinate(5.4201977, -1.3800000),
      new Coordinate(5.4195490, -1.3840000),
      new Coordinate(5.4187309, -1.3900000),
      new Coordinate(5.4182880, -1.3940000),
      new Coordinate(5.4177757, -1.4000000),
      new Coordinate(5.4175350, -1.4040000),
      new Coordinate(5.4173246, -1.4100000),
      new Coordinate(5.4172943, -1.4160000),
      new Coordinate(5.4174441, -1.4220000),
      new Coordinate(5.4177745, -1.4280000),
      new Coordinate(5.4180954, -1.4320000),
      new Coordinate(5.4187289, -1.4380000),
      new Coordinate(5.4195464, -1.4440000),
      new Coordinate(5.4205503, -1.4500000),
      new Coordinate(5.4217433, -1.4560000),
      new Coordinate(5.4219804, -1.4580000),
      new Coordinate(5.4222806, -1.4640000),
      new Coordinate(5.4225814, -1.4680000),
      new Coordinate(5.4231843, -1.4740000),
      new Coordinate(5.4236883, -1.4780000),
      new Coordinate(5.4242745, -1.4820000),
      new Coordinate(5.4261607, -1.4940000),
      new Coordinate(5.4272588, -1.5000000),
      new Coordinate(5.4285478, -1.5060000),
      new Coordinate(5.4295150, -1.5100000),
      new Coordinate(5.4305700, -1.5140000),
      new Coordinate(5.4317143, -1.5180000),
      new Coordinate(5.4336017, -1.5240000),
      new Coordinate(5.4349765, -1.5280000),
      new Coordinate(5.4364471, -1.5320000),
      new Coordinate(5.4380156, -1.5360000),
      new Coordinate(5.4405576, -1.5420000),
      new Coordinate(5.4423826, -1.5460000),
      new Coordinate(5.4448159, -1.5510000),
      new Coordinate(5.4468887, -1.5550000),
      new Coordinate(5.4496439, -1.5600000),
      new Coordinate(5.4519848, -1.5640000),
      new Coordinate(5.4550904, -1.5690000),
      new Coordinate(5.4577253, -1.5730000),
      new Coordinate(5.4605011, -1.5770000),
      new Coordinate(5.4634249, -1.5810000),
      new Coordinate(5.4665051, -1.5850000),
      new Coordinate(5.4697510, -1.5890000),
      new Coordinate(5.4731735, -1.5930000),
      new Coordinate(5.4767848, -1.5970000),
      new Coordinate(5.4805993, -1.6010000),
      new Coordinate(5.4846337, -1.6050000),
      new Coordinate(5.4889078, -1.6090000),
      new Coordinate(5.4922847, -1.6120000),
      new Coordinate(5.4970381, -1.6160000),
      new Coordinate(5.5008095, -1.6190000),
      new Coordinate(5.5034312, -1.6210000),
      new Coordinate(5.5061455, -1.6230000),
      new Coordinate(5.5157525, -1.6312621),
      new Coordinate(5.5422447, -1.6466244),
      new Coordinate(5.5697853, -1.6598139),
      new Coordinate(5.6013621, -1.6691743),
      new Coordinate(5.6340486, -1.6732196)};
  private static final Coordinate[] featurePole = {
      new Coordinate(-56.1637542, -84.1609504),
      new Coordinate(-56.1667049, -84.1625689),
      new Coordinate(-56.1787251, -84.1629872),
      new Coordinate(-56.1927104, -84.1617744),
      new Coordinate(-56.2077061, -84.1607594),
      new Coordinate(-56.2176914, -84.1597379),
      new Coordinate(-56.2167078, -84.1585240),
      new Coordinate(-56.2027676, -84.1566761),
      new Coordinate(-56.1898007, -84.1555635),
      new Coordinate(-56.1788280, -84.1548446),
      new Coordinate(-56.1658899, -84.1547388),
      new Coordinate(-56.1598778, -84.1550362),
      new Coordinate(-56.1608058, -84.1572730),
      new Coordinate(-56.1617903, -84.1590114),
      new Coordinate(-56.1637542, -84.1609504)};
}
