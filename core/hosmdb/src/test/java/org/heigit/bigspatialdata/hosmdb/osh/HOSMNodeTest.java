package org.heigit.bigspatialdata.hosmdb.osh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.heigit.bigspatialdata.hosmdb.osm.OSMNode;
import org.junit.Test;

public class HOSMNodeTest {

	public static final int USER_A = 1;
	public static final int USER_B = 2;
	public static final int[] TAGS_A = new int[] { 1, 1 };
	public static final int[] TAGS_B = new int[] { 2, 2 };
	public static final long[] LONLAT_A = new long[] { 86756350l, 494186210l };
	public static final long[] LONLAT_B = new long[] { 87153340l, 494102830l };

	
	@Test
	public void testBuild() throws IOException{
		List<OSMNode> versions = new ArrayList<>();
	    
	    versions.add(new OSMNode(123l,1,1l,0l,USER_A,TAGS_A,LONLAT_A[0], LONLAT_A[1]));
	    versions.add(new OSMNode(123l,-2,2l,0l,USER_A,TAGS_A,LONLAT_A[0], LONLAT_A[1]));
	    
	    
	    HOSMNode hnode = HOSMNode.build(versions);    
	    assertNotNull(hnode);
	    
	    List<OSMNode> v = hnode.getVersions();
	    assertNotNull(v);
	    assertEquals(2, v.size());
	}
	
	@Test
	public void testCompact() {
		fail("Not yet implemented");
	}

}
