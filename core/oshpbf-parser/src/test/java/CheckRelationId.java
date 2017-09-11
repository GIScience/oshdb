
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfRelation;

public class CheckRelationId {

  public static void main(String[] args) throws FileNotFoundException, IOException {
    try {
      final String filename = new File(BlockCount.class.getResource("/org/heigit/bigspatialdata/oshpbf/mapreduce/maldives.osh.pbf").toURI()).toString();
      final OsmPrimitiveBlockIterator pbfBlock = new OsmPrimitiveBlockIterator(filename);
      final OsmPbfIterator osmIterator = new OsmPbfIterator(pbfBlock);
      final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);

      while (oshIterator.hasNext()) {
        List<OSMPbfEntity> e = oshIterator.next();

        if (e.get(0).getType() != Type.RELATION) {
          continue;
        }
        long id = e.get(0).getId();

        List<OSMPbfRelation> versions = (List<OSMPbfRelation>) (List) e;

        for (OSMPbfRelation osmPbfRelation : versions) {
          for (OSMPbfRelation.OSMMember member : osmPbfRelation.getMembers()) {
            if (member.getType() == OSMType.RELATION.intValue()) {
              if (member.getMemId() == id) {
                System.out.println("selbst referenz! " + id);
              } else if (member.getMemId() > id) {
                System.out.printf("referenz auf eine hoehere id %d -> %d\n", id, member.getMemId());
              }
            }
          }
        }
      }

      System.out.println("Fertig!");
    } catch (URISyntaxException ex) {
      Logger.getLogger(CheckRelationId.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

}
