package org.heigit.bigspatialdata.oshdb.tool.etl.transform.data;

import java.util.ArrayList;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osh.OSHWay;

public class CellWay implements Comparable<CellWay> {

  private final CellInfo info;
  
private final List<OSHWay> ways = new ArrayList<>();
  
  private long minTimestamp;
  private long minId;
  
  public CellWay(final long cellId, final int zoomLevel){
    this.info = new CellInfo(cellId,zoomLevel);
  }
  
  
  public CellInfo info(){
    return info;
  }
 
  public void add(OSHWay way, long minTimestamp){
    this.ways.add(way);
    minId = Math.min(minId, way.getId());
    minTimestamp = Math.min(this.minTimestamp,minTimestamp);
  }
  
  public List<OSHWay> getWays(){
    return ways;
  }
  
  
  public long minId(){
    return minId;
  }
  
  public long minTimestamp(){
    return minTimestamp;
  }

  @Override
  public int compareTo(CellWay o) {
   return info.compareTo(o.info);
  }
  
  @Override
  public int hashCode() {
    return info.hashCode();
  }
}
