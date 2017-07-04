package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

public class TimeStampValuesWeights {

  public final Map<Long, ValueWeight> map = new HashMap<>();
  
  ValueWeight get(long timeStamp){
    
    if (map.containsKey(timeStamp)){
      return map.get(timeStamp);
    }
    else {
      map.put(timeStamp, new ValueWeight());
      return map.get(timeStamp);
    }
    
  }
  
}
