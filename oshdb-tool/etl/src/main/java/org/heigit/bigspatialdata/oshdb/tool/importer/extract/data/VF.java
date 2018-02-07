package org.heigit.bigspatialdata.oshdb.tool.importer.extract.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;

public class VF {
  public final String value;
  public final int freq;

  public VF(String value, int freq) {
    this.value = value;
    this.freq = freq;
  }

  public int freq() {
    return freq;
  }

  public void write(DataOutput out) throws IOException{
    out.writeUTF(value);
    out.writeInt(freq);
  }
  
  public static VF read(DataInput in) throws IOException{
    final String value = in.readUTF();
    final int freq = in.readInt();
    return new VF(value,freq);
  }
  
  @Override
  public String toString() {
    return String.format("(%s:%d)", value,freq);
  }
  
  public long estimateSize() {
    final long size = SizeEstimator.estimatedSizeOf("") // obj_overhead
                                                              // (vf)
        + SizeEstimator.estimatedSizeOf(value) // value
        + 4; // freq
    return size;
  }
}
