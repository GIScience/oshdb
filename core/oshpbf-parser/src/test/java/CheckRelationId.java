import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfRelation;

public class CheckRelationId {

  public static void main(String[] args) throws FileNotFoundException, IOException {
    final String filename = "/home/rtroilo/heigit_git/data/venice.osh.pbf";
    final OsmPrimitiveBlockIterator pbfBlock = new OsmPrimitiveBlockIterator(filename);
    final OsmPbfIterator osmIterator = new OsmPbfIterator(pbfBlock);
    final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);
    
    while(oshIterator.hasNext()){
      List<OSMPbfEntity> e = oshIterator.next();
       
      if(e.get(0).getType() != Type.RELATION)
        continue;
      long id = e.get(0).getId();
      
      List<OSMPbfRelation> versions = (List<OSMPbfRelation>) (List)e;
      
      for (OSMPbfRelation osmPbfRelation : versions) {
        for(OSMPbfRelation.OSMMember member : osmPbfRelation.getMembers()){
          if(member.getType() == 2){
            if(member.getMemId() == id){
              System.out.println("selbst referenz! "+id);
            }else if (member.getMemId() > id){
              System.out.printf("referenz auf eine hoere id %d -> %d\n",id,member.getMemId());
            }
          }
        }
      }
    }
    
    System.out.println("Fertig!");
  }

}
