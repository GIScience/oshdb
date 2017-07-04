package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Map;

public class CellTimeStamps {
  
  private Map<Long, TimeStampValuesWeights> cellTimeStamps;
  
  TimeStampValuesWeights get(long cellId){
    TimeStampValuesWeights ret = cellTimeStamps.get(cellId);
    
    if(ret== null){
      ret =  new TimeStampValuesWeights();
      cellTimeStamps.put(cellId, ret);
    }
    
    return ret;
  }
  
}
