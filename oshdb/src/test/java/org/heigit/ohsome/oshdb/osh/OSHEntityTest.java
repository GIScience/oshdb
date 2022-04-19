package org.heigit.ohsome.oshdb.osh;

import static org.heigit.ohsome.oshdb.osh.OSHNodeTest.buildOSHNode;
import static org.heigit.ohsome.oshdb.osm.OSM.node;
import static org.heigit.ohsome.oshdb.osm.OSM.relation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class OSHEntityTest {

  @Test
  public void testHashCodeEquals() throws IOException {
    var expected = buildOSHNode(
        node(123L, 1, 1L, 0L, 1, new int[0], 0, 0)
    );

    var a = buildOSHNode(
        node(123L, 1, 1L, 0L, 1, new int[0], 0, 0)
    );

    var b = OSHRelationImpl.build(Lists.newArrayList(
        relation(123L, 1, 3333L, 4444L, 23, new int[0],
            new OSMMember[0])), Collections.emptyList(), Collections.emptyList());

    assertEquals(expected.hashCode(), a.hashCode());
    assertNotEquals(expected.hashCode(), b.hashCode());

    assertEquals(expected, a);
    assertNotEquals(expected, b);
  }

}
