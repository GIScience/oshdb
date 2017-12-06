import java.io.FileNotFoundException;
import java.io.IOException;

import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfTag;

public class HongzhouTest {

 
  public static void main(String[] args) throws FileNotFoundException, IOException {
    
    if(args.length < 2){
      System.out.println("missing arguments: pbffile, idToPrint");
    }
      
    
    String filename = args[0]; //"/home/rtroilo/heigit_git/data/hangzhou-relation.osh.pbf";
    
    long id = Long.valueOf(args[1]);
        
    final OsmPrimitiveBlockIterator pbfBlock = new OsmPrimitiveBlockIterator(filename);
    final OsmPbfIterator osmIterator = new OsmPbfIterator(pbfBlock);

    
    
    int count = 0;
    System.out.println(pbfBlock.getHeaderInfo());
    
    while(osmIterator.hasNext()){
        OSMPbfEntity entity = osmIterator.next();
        if(entity.getId() == id){
          print(entity);
        }
        if(entity.getId() > id){
          break;
        }
    }
  }
  
  private static void print(OSMPbfEntity entity){
    long id = entity.getId();
    
    
    System.out.printf("%s %s -> %s\n",entity.getId(),entity.getTimestamp(),tags(entity));
    
  }
  
  private static String tags(OSMPbfEntity entity){
    
    StringBuilder sb = new StringBuilder();
    
    for(OSMPbfTag tag :entity.getTags()){
      sb.append(" ");
      sb.append(tag.getKey()).append("=").append(tag.getValue());
    }
    return sb.toString().trim();
  }

}
