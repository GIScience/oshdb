package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

public class TimeStampValuesWeights {
  
  private Map<Long, ValueWeight> timeStampValuesWeights;
  
  ValueWeight get(long timeStamp){
    ValueWeight ret = timeStampValuesWeights.get(timeStamp);
    
    if(ret == null){
      ret = new ValueWeight();
      timeStampValuesWeights.put(timeStamp, ret);
      
    }
    return ret;
  }

    
  
}
