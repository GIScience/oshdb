package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.HashMap;
import java.util.Map;

public class CellTimeStamps {

  public final Map<Long, TimeStampValuesWeights> map = new HashMap<Long, TimeStampValuesWeights>();
  
  TimeStampValuesWeights get(long cellId){
    
    if ( map.containsKey(cellId) ){
    
    TimeStampValuesWeights ret = map.get(cellId);
    return ret;
    }
    
    else {

      map.put(cellId, new TimeStampValuesWeights());
      TimeStampValuesWeights ret = map.get(cellId);
      return ret;
  
  }
    
    
  }
  
}
