import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;

public class FindNode {

  public static void main(String[] args) throws FileNotFoundException, IOException {
    
    
    
    final String filename = "/home/rtroilo/heigit_git/data/venice.osh.pbf";
    final OsmPrimitiveBlockIterator pbfBlock = new OsmPrimitiveBlockIterator(filename);
    final OsmPbfIterator osmIterator = new OsmPbfIterator(pbfBlock);
    final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);
    
    while(oshIterator.hasNext()){
      List<OSMPbfEntity> e = oshIterator.next();
      
      if(e.get(0).getId() == 883702275){
        System.out.println(e);
        break;
      }
       
      if(e.get(0).getType() == Type.WAY)
        break;
    }
    

  }

}
