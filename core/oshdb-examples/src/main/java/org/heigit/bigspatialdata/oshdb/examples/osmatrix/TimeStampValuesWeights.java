package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

public class TimeStampValuesWeights {
  
  public TimeStampValuesWeights() {
    //super();
    this.timeStampValuesWeights = new HashMap<Long,ValueWeight>();
  }

  private Map<Long, ValueWeight> timeStampValuesWeights;
  
  ValueWeight get(long timeStamp){
    
    if (timeStampValuesWeights.containsKey(timeStamp)){
      return timeStampValuesWeights.get(timeStamp);
    }
    else {
      timeStampValuesWeights.put(timeStamp, timeStampValuesWeights.get(timeStamp));
      return timeStampValuesWeights.get(timeStamp);
    }
    
  }
  
}
