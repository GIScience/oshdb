package org.heigit.bigspatialdata.oshdb.tool.importer.transform.reader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.oshdb.TransfomRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.ZGrid;

public class TransformRelationReaders implements Closeable {
  private static class TransformRelationReader extends TransformReader<TransfomRelation> {

    public TransformRelationReader(Path path) throws IOException {
      super(path);
    }

    @Override
    protected TransfomRelation getInstance(byte[] data, int offset, int length, long baseId, long baseTimestamp, long baseLongitude, long baseLatitude) throws IOException {
      return TransfomRelation.instance(data, offset, length,baseId,baseTimestamp,baseLongitude,baseLatitude);
    }
  }
  final List<TransformRelationReader> readers;
  final PriorityQueue<TransformRelationReader> queue;
  final List<TransformRelationReader> next;
  
  public TransformRelationReaders(Path... path) throws IOException{
    queue = new PriorityQueue<>(path.length, (a,b) -> ZGrid.ORDER_DFS_TOP_DOWN.compare(a.getCellId(), b.getCellId()));
    next = new ArrayList<>(path.length);
    readers = new ArrayList<>(path.length);
    for(Path p : path){
      TransformRelationReader reader = new TransformRelationReader(p);
      readers.add(reader);
      if(reader.hasNext()){
        reader.next();
        queue.add(reader);
      }
    }
  }
  
  @Override
  public void close() throws IOException {
     readers.forEach(reader -> {
       try {
        reader.close();
      } catch (IOException e) {}
     });
  }
  
  public boolean hasNext(){
    return queue.size() > 0;
  }
  
  public long getCellId(){
    if(queue.isEmpty()){
      return Long.MIN_VALUE;
    }
    return queue.peek().getCellId();
  }
  
  public Set<TransfomRelation> next(){
    next.add(queue.poll());
    final long cellId = next.get(0).getCellId();
    while(!queue.isEmpty() && cellId  == queue.peek().cellId){
      next.add(queue.poll());
    }
    //final int size = next.stream().mapToInt(TransformReader::getSize).sum();
    final Set<TransfomRelation> ret = new TreeSet<>((a,b) -> Long.compare(a.getId(), b.getId()));
    next.stream().map(TransformReader::entities).forEach(ret::addAll);    
    next.stream().filter(TransformReader::hasNext).map(r -> {r.next(); return r;}).forEach(r -> queue.add(r));
    next.clear(); 
    return ret;
  }
  
}
