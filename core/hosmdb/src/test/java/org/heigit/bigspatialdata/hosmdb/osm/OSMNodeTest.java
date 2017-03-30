package org.heigit.bigspatialdata.hosmdb.osm;

import static org.junit.Assert.*;

import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.OSMCommonProperties;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfNode;
import org.junit.Test;

public class OSMNodeTest {


  @Test
  public void testIsVisible() {
    OSMCommonProperties props = new OSMCommonProperties();
    props.id = 27176251;;
    props.version = 5;
    props.timestamp = 1342467946000l;
    props.changeset = 12250605;
    props.visible = false;

    OSMPbfNode pbfNode = new OSMPbfNode(props, 214748364700l, 214748364700l);
    OSMNode node = getNode(pbfNode);

    assertFalse(node.isVisible());

  }

  private OSMNode getNode(OSMPbfNode entity) {
    return new OSMNode(entity.getId(), //
        entity.getVersion() * (entity.getVisible() ? 1 : -1), //
        entity.getTimestamp(), //
        entity.getChangeset(), //
       0, //
        new int[0], //
        entity.getLongitude(), entity.getLatitude());
  }

  @Test
  public void testEqualsToOSMNode() {

    long id = 123;
    int version = 1;
    long timestamp = 310172400000l;
    long changeset = 4444;
    int userId = 23;
    int[] tags = new int[] {1, 1, 2, 2, 3, 3};
    long longitude = 86809727l;
    long latitude = 494094984l;



    OSMNode a = new OSMNode(id, version, timestamp, changeset, userId, tags, longitude, latitude);
    OSMNode b = new OSMNode(id, version, timestamp, changeset, userId, tags, longitude, latitude);
    assertTrue(a.equalsTo(b));
  }

  @Test
  public void testCompareTo() {
    long id = 123;
    int version = 1;
    long timestamp = 310172400000l;
    long changeset = 4444;
    int userId = 23;
    int[] tags = new int[] {1, 1, 2, 2, 3, 3};
    long longitude = 86809727l;
    long latitude = 494094984l;

    OSMNode a = new OSMNode(id, version, timestamp, changeset, userId, tags, longitude, latitude);

    OSMNode b;

    b = new OSMNode(id, version + 2, timestamp, changeset, userId, tags, longitude, latitude);

    assertTrue(a.compareTo(b) < 0);

  }

}
