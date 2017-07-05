package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.util.ArrayList;

public class ValueWeight {
  public final ArrayList<Double> valueWeight = new ArrayList<Double>(2);
  
  public ValueWeight() {
    valueWeight.add(0.0);
    valueWeight.add(0.0);
    
  }
  public void setValueWeight(double value, double weight){
    valueWeight.set(0, value);
    valueWeight.set(1, weight);
    
  }

  public double getValue() {
    return valueWeight.get(0);
  }

  public void setValue(Double value) {
     valueWeight.set(0, value);
  }
  
  public double getWeight(){
    return valueWeight.get(1);
    
  }
  
  public void setWeight(Double weight){
    valueWeight.set(1, weight);
  }
  
  
  
}
