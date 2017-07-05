package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class AttributeCells {
     
  public final Map<Integer,CellTimeStamps> map = new HashMap<Integer, CellTimeStamps>();
  
  CellTimeStamps get(int attributeId){
    
    if (map.containsKey(attributeId)){
      return map.get(attributeId);
    }
    else
    {
      map.put(attributeId, new CellTimeStamps());
      return map.get(attributeId); 
    }
      
  }
  
  
}
