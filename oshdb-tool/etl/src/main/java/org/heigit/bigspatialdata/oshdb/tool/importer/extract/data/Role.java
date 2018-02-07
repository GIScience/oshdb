package org.heigit.bigspatialdata.oshdb.tool.importer.extract.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;

public class Role {
  public final String role;
  public final int freq;

  public Role(String role, int frequency) {
    this.role = role;
    this.freq = frequency;
  }
  
  public long estimateSize(){
    return SizeEstimator.estimatedSizeOf(role)+4;
  }
  
  public static final Comparator<Role> comparatorByFrequency = (a, b) -> {
    int c = Integer.compare(a.freq,b.freq);
    if (c != 0)
      return c * -1;
    return a.role.compareTo(b.role);
  };
    
  public void write(DataOutput out) throws IOException{
    out.writeUTF(role);
    out.writeInt(freq);
  }
  
  public static Role read(DataInput in) throws IOException{
    final String role = in.readUTF();
    final int freq = in.readInt();
    return new Role(role,freq);
  }
}
