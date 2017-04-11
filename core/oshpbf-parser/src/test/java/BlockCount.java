
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Spliterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;

public class BlockCount {

  public static void main(String[] args) throws FileNotFoundException, IOException {
    try {
      String filename = new File(BlockCount.class.getResource("/org/heigit/bigspatialdata/oshpbf/mapreduce/maldives.osh.pbf").toURI()).toString();

      Spliterator<String> ou;

      final OsmPrimitiveBlockIterator pbfBlock = new OsmPrimitiveBlockIterator(filename);
      final OsmPbfIterator osmIterator = new OsmPbfIterator(pbfBlock);
      final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);

      int countBlocks = 0;
      System.out.println(pbfBlock.getHeaderInfo());

      while (pbfBlock.hasNext()) {
        pbfBlock.next();
        countBlocks++;
      }

      System.out.println("Count " + countBlocks);
    } catch (URISyntaxException ex) {
      Logger.getLogger(BlockCount.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

}
