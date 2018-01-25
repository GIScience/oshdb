package org.heigit.bigspatialdata.oshdb.tool.etl.transform.data;

public class CellInfo implements Comparable<CellInfo>{
  
  private final long id;
  private final int zoomLevel;
  
  public CellInfo(final long id, final int zoomLevel) {
   this.id = id;
   this.zoomLevel = zoomLevel;
  }

  public long getId() {
    return id;
  }

  public int getZoomLevel() {
    return zoomLevel;
  }

  @Override
  public int compareTo(CellInfo o) {
    int c = Integer.compare(zoomLevel, o.zoomLevel);
    if(c == 0){
      c = Long.compare(id, o.id);
    }
    return c;
  }
  
  
}
