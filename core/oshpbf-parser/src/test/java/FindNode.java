
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;

public class FindNode {

  public static void main(String[] args) throws FileNotFoundException, IOException {

    try {
      final String filename = new File(BlockCount.class.getResource("/org/heigit/bigspatialdata/oshpbf/mapreduce/maldives.osh.pbf").toURI()).toString();
      final OsmPrimitiveBlockIterator pbfBlock = new OsmPrimitiveBlockIterator(filename);
      final OsmPbfIterator osmIterator = new OsmPbfIterator(pbfBlock);
      final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);

      while (oshIterator.hasNext()) {
        List<OSMPbfEntity> e = oshIterator.next();

        if (e.get(0).getId() == 883702275) {
          System.out.println(e);
          break;
        }

        if (e.get(0).getType() == Type.WAY) {
          break;
        }
      }
    } catch (URISyntaxException ex) {
      Logger.getLogger(FindNode.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

}
