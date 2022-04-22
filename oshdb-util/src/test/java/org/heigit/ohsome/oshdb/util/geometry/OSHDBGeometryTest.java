package org.heigit.ohsome.oshdb.util.geometry;

import static org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser.toOSHDBTimestamp;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.common.collect.ListMultimap;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.locationtech.jts.geom.Geometry;

public abstract class OSHDBGeometryTest {
  protected final OSMXmlReader testData = new OSMXmlReader();
  protected final ListMultimap<Long, OSMNode> nodes;
  protected final ListMultimap<Long, OSMWay> ways;
  protected final ListMultimap<Long, OSMRelation> relations;
  protected final TagInterpreter areaDecider;

  protected OSHDBGeometryTest(String testdata) {
    testData.add(testdata);
    nodes = testData.nodes();
    ways = testData.ways();
    relations = testData.relations();
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  protected OSMNode nodes(long id, int index) {
    return nodes.get(id).get(index);
  }

  protected OSMWay ways(long id, int index) {
    return ways.get(id).get(index);
  }

  protected OSMRelation relations(long id, int index) {
    return relations.get(id).get(index);
  }

  protected Geometry buildGeometry(OSMEntity entity) {
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    return buildGeometry(entity, timestamp);
  }

  protected Geometry buildGeometry(OSMEntity entity, String timestamp) {
    return buildGeometry(entity, toOSHDBTimestamp(timestamp));
  }

  protected Geometry buildGeometry(OSMEntity entity, OSHDBTimestamp timestamp) {
    return assertDoesNotThrow(() ->
      OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider));
  }
}
