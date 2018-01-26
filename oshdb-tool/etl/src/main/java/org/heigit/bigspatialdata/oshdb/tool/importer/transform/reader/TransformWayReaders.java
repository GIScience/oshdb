package org.heigit.bigspatialdata.oshdb.tool.importer.transform.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;

public class TransformWayReaders {
  private static class TransformWayReader extends TransformReader<TransformOSHWay> {

    public TransformWayReader(Path path) throws IOException {
      super(path);
    }

    @Override
    protected TransformOSHWay getInstance(byte[] data, int offset, int length,long baseId, long baseTimestamp, long baseLongitude, long baseLatitude) throws IOException {
      return TransformOSHWay.instance(data, offset, length,baseId,baseTimestamp,baseLongitude,baseLatitude);
    }
    
  }
  
  final PriorityQueue<TransformWayReader> queue;
  final List<TransformWayReader> next;
  
  public TransformWayReaders(Path... path) throws IOException{
    queue = new PriorityQueue<>(path.length, (a,b) -> ZGrid.ORDER_DFS_TOP_DOWN.compare(a.getCellId(), b.getCellId()));
    next = new ArrayList<>(path.length);
    for(Path p : path){
      TransformWayReader reader = new TransformWayReader(p);
      if(reader.hasNext()){
        reader.next();
        queue.add(reader);
      }
    }
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
  
  public Set<TransformOSHWay> next(){
    next.add(queue.poll());
    final long cellId = next.get(0).getCellId();
    while(!queue.isEmpty() && cellId  == queue.peek().cellId){
      next.add(queue.poll());
    }
    //final int size = next.stream().mapToInt(TransformReader::getSize).sum();
    final Set<TransformOSHWay> ret = new TreeSet<>((a,b)-> Long.compare(a.getId(), b.getId()));
    next.stream().map(TransformReader::entities).forEach(ret::addAll);    
    next.stream().filter(TransformReader::hasNext).map(r -> {r.next(); return r;}).forEach(r -> queue.add(r));
    next.clear(); 
    return ret;
  }
}
