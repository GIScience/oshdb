package org.heigit.bigspatialdata.oshdb.tool.importer.transform.reader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osh2.OSHEntity2;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;


public abstract class TransformReader<T extends OSHEntity2> {
  
  public final Path path;
  private final RandomAccessFile raf;
  private final long end;
  protected final FileChannel channel;
  
  private final ByteBuffer header = ByteBuffer.allocateDirect(8 + 4 + 4);
  
  protected long pos = 0;
  
  public long cellId = Long.MIN_VALUE;
  private int size = 0;
  private int bytes = 0;
  
  public TransformReader(Path path) throws IOException {
    this.path = path;
    this.raf = new RandomAccessFile(path.toFile(), "r");
    this.end = raf.length();
    this.channel = raf.getChannel();
  }

  
  public long getCellId(){
    return cellId;
  }
  
  public int getSize(){
    return size;
  }
  
  public Path getPath(){
    return path;
  }
  
  protected void readHeader() throws IOException{
    pos += bytes;
    header.clear();
    channel.read(header,pos);
    pos += header.capacity();  
  
    header.flip();
    this.cellId = header.getLong();
    this.size = header.getInt();
    this.bytes = header.getInt();
    if(bytes < 0)
      System.out.println("bytes is negative");
  }
  
  public boolean hasNext(){
    return pos+bytes < end;
  }
  
  public TransformReader next(){
    try {
      readHeader();
      return this;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
   
  }
  
  public Set<T> entities(){
    try {
    final ByteBuffer data = ByteBuffer.allocateDirect(bytes);
    channel.read(data, pos);
    data.flip();
    
    final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(cellId);
    final long baseLongitude = bbox.getMinLonLong();
    final long baseLatitude = bbox.getMinLatLong();
    
    final Set<T> ret = new TreeSet<>((a,b) -> Long.compare(a.getId(), b.getId()));
    long id = 0;
    while(data.hasRemaining()){
      int length = data.getInt();
      byte[] content = new byte[length];
      data.get(content);
      T node = getInstance(content, 0, length,id,0,baseLongitude,baseLatitude);
      id = node.getId();
      ret.add(node);
    }
    return ret;
    }catch(IOException e){
      throw new RuntimeException(e);
    }
  }
  
  protected abstract T getInstance(byte[] data, int offset, int length, long baseId, long baseTimestamp, long baseLongitude, long baseLatitude) throws IOException;
}


