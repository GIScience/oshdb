package org.heigit.bigspatialdata.oshdb.osh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.junit.Test;

public class OSHWayTest {
  
  OSHNode node100 = buildHOSMNode(
      Arrays.asList(new OSMNode(100l, 1, 1l, 0l, 123, new int[] {1, 2}, 494094984l, 86809727l)));
  OSHNode node102 = buildHOSMNode(
      Arrays.asList(new OSMNode(102l, 1, 1l, 0l, 123, new int[] {2, 1}, 494094984l, 86809727l)));
  OSHNode node104 = buildHOSMNode(
      Arrays.asList(new OSMNode(104l, 1, 1l, 0l, 123, new int[] {2, 4}, 494094984l, 86809727l)));

  @Test
  public void testGetNodes() throws IOException {
    List<OSMWay> versions = new ArrayList<>();
    versions.add(
        new OSMWay(123, 1, 3333l, 4444l, 23, new int[] {1,1,2,1}, new OSMMember[] {new OSMMember(102, 0, 0),new OSMMember(104,0,0)}));
    versions.add(
        new OSMWay(123, 3, 3333l, 4444l, 23, new int[] {1,1,2,2}, new OSMMember[] {new OSMMember(100, 0, 0),new OSMMember(104,0,0)}));
    
    OSHWay hway = OSHWay.build(versions, Arrays.asList(node100,node102,node104));
    assertNotNull(hway);
    
    List<OSHNode> nodes = hway.getNodes();
    assertEquals(3, nodes.size());
        
  }
  
  
  @Test
  public void testCreateGeometrey() throws IOException {
	  List<OSMWay> versions = new ArrayList<>();
	    versions.add(
	        new OSMWay(123, 1, 3333l, 4444l, 23, new int[] {1,1,2,1}, new OSMMember[] {new OSMMember(102, 0, 0),new OSMMember(104,0,0)}));
	    versions.add(
	        new OSMWay(123, 3, 3333l, 4444l, 23, new int[] {1,1,2,2}, new OSMMember[] {new OSMMember(100, 0, 0),new OSMMember(104,0,0)}));
	    
	    OSHWay hway = OSHWay.build(versions, Arrays.asList(node100,node102,node104));
	    assertNotNull(hway);
	    
	    
	    Iterator<OSMWay> ways = hway.iterator();
	   
	    OSMWay w = ways.next();
	    
	    
	    OSMMember[] members = w.getRefs();
	    members[0].getEntity();
	  
	    
	    
	    List<OSHNode> nodes = hway.getNodes();
	  
  }

  @Test
  public void testWithMissingNode() throws IOException{
    List<OSMWay> versions = new ArrayList<>();
    versions.add(
        new OSMWay(123, 3, 3333l, 4444l, 23, new int[] {1,1,2,2}, new OSMMember[] {new OSMMember(100, 0, 0),new OSMMember(104,0,0)}));
    versions.add(
        new OSMWay(123, 1, 3333l, 4444l, 23, new int[] {1,1,2,1}, new OSMMember[] {new OSMMember(102, 0, 0),new OSMMember(104,0,0)}));

    
    OSHWay hway = OSHWay.build(versions, Arrays.asList(node100,node104));
    assertNotNull(hway);
    
    List<OSHNode> nodes = hway.getNodes();
    assertEquals(2, nodes.size());
    


    OSMWay way;
    OSMMember[] members;
    Iterator<OSMWay> itr = hway.iterator();
    assertTrue(itr.hasNext());
    way = itr.next();
    members = way.getRefs();
    assertEquals(2,members.length);
    assertEquals(100, members[0].getId());
    assertEquals(104, members[1].getId());
    
    assertTrue(itr.hasNext());
    way = itr.next();
    members = way.getRefs();
    assertEquals(2,members.length);
    
    assertEquals(102, members[0].getId());
    assertEquals(104, members[1].getId());
  }
  
  
  static OSHNode buildHOSMNode(List<OSMNode> versions){
    try {
      return OSHNode.build(versions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }


}
