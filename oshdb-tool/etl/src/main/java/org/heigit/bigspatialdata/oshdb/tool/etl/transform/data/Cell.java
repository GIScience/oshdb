package org.heigit.bigspatialdata.oshdb.tool.etl.transform.data;

import java.util.ArrayList;
import java.util.List;

public class Cell<TYPE> implements Comparable<Cell>{
  
  private int level;
  private long id;
  
  private long minTimestamp;
  private long minId;
  
  private final List<TYPE> nodes = new ArrayList<>();

  @Override
  public int compareTo(Cell o) {
    // TODO Auto-generated method stub
    return 0;
  }

}
