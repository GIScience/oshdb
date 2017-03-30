import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Spliterator;

import org.heigit.bigspatialdata.oshpbf.OSHPbfParser;
import org.heigit.bigspatialdata.oshpbf.OshPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPbfBlocks;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;

public class BlockCount {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		String filename = "target/test-classes/data/venice.osh.pbf";
		
	
		Spliterator<String> ou;
		
		
		
		final OsmPrimitiveBlockIterator pbfBlock = new OsmPrimitiveBlockIterator(filename);
		final OsmPbfIterator osmIterator = new OsmPbfIterator(pbfBlock);
		final OshPbfIterator oshIterator = new OshPbfIterator(osmIterator);
		

		
		
		int countBlocks = 0;
		System.out.println(pbfBlock.getHeaderInfo());
		
		while(pbfBlock.hasNext()){
			pbfBlock.next();
			countBlocks++;
		}
		
		
		System.out.println("Count "+countBlocks);

	}

}
