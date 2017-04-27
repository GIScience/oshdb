package org.heigit.bigspatialdata.oshdb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfNode;

public class TestBuildOSHNodeFromPBF {
	
	private static OSMNode getNode(OSMPbfNode entity) {
	    return new OSMNode(entity.getId(), //
	        entity.getVersion() * (entity.getVisible()?1:-1), //
	        entity.getTimestamp(), //
	        entity.getChangeset(), //
	        entity.getUser().getId(), //
	        new int[0], //
	        entity.getLongitude(), entity.getLatitude());
	  }

	public static void main(String[] args) throws FileNotFoundException, IOException {
		final String filename = "/home/rtroilo/heigit_git/martin/kathmandu.osh.pbf";
	    final OsmPrimitiveBlockIterator pbfBlock = new OsmPrimitiveBlockIterator(filename);
	    final OsmPbfIterator osmIterator = new OsmPbfIterator(pbfBlock);
	    final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);
	    
	    while(oshIterator.hasNext()){
	      List<OSMPbfEntity> e = oshIterator.next();
	      
	      if(e.get(0).getType() != Type.NODE)
	    	  break;
	      
	      if(e.get(0).getId() == 3718143950l){
	    	List<OSMNode> nodes = new ArrayList<>();
	        for(OSMPbfEntity osm : e){
	        	OSMNode n = getNode((OSMPbfNode) osm);
	        	nodes.add(n);
	        }
	        Collections.sort(nodes, Collections.reverseOrder());
	        nodes.forEach(System.out::println);
	        
	        System.out.println();
	        OSHNode hnode = OSHNode.build(nodes);
	        for(OSMNode osm : hnode){
	        	System.out.println(osm);
	        }
	    	  
	        break;
	      }
	       
	      
	    }

	}

}
