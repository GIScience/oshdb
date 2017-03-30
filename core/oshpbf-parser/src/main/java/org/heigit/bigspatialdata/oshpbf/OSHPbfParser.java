package org.heigit.bigspatialdata.oshpbf;

import java.io.Closeable;
import java.io.File;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;


public class OSHPbfParser implements Closeable, Iterator<List<OSMPbfEntity>>{
  
  
  private final OsmPrimitiveBlockIterator blockIterator;
  private final OsmPbfIterator osmIterator;
  private final OshPbfIterator oshIterator;
  private final String filename;


  public OSHPbfParser(String filename) throws FileNotFoundException, IOException{
    this(new File(filename));
  }
  
  public OSHPbfParser(File file) throws FileNotFoundException, IOException{
    filename = file.getName();
    blockIterator = new OsmPrimitiveBlockIterator(file);
    osmIterator = new OsmPbfIterator(blockIterator);
    oshIterator = new OshPbfIterator(osmIterator);
  }
  
  public OSHPbfParser(InputStream is) throws IOException{
    filename = null;
    blockIterator = new OsmPrimitiveBlockIterator(is);
    osmIterator = new OsmPbfIterator(blockIterator);
    oshIterator = new OshPbfIterator(osmIterator);
  }
  
 public long getCurrentBlockStartPos(){
   return blockIterator.getBlockPos();
 }

  @Override
  public void close() throws IOException {
    blockIterator.close();
  }

  @Override
  public boolean hasNext() {
    return oshIterator.hasNext();
  }

  @Override
  public List<OSMPbfEntity> next() {
    return oshIterator.next();
  }
}
