package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.HashMap;
import java.util.Map;

public class CellTimeStamps {
  
  public CellTimeStamps() {
   // super();
    this.cellTimeStamps = new HashMap<Long, TimeStampValuesWeights>();
  }

  private Map<Long, TimeStampValuesWeights> cellTimeStamps;
  
  TimeStampValuesWeights get(long cellId){
    
    if ( cellTimeStamps.containsKey(cellId) ){
    
    TimeStampValuesWeights ret = cellTimeStamps.get(cellId);
    return ret;
    }
    
    else {

      cellTimeStamps.put(cellId, cellTimeStamps.get(cellId));
      TimeStampValuesWeights ret = cellTimeStamps.get(cellId);
      return ret;
  
  }
    
    
  }
  
}
