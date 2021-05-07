package org.heigit.ohsome.oshdb.tool.importer.extract.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.heigit.ohsome.oshdb.tool.importer.util.SizeEstimator;

public class ValueFrequency {
  public final String value;
  public final int freq;

  public ValueFrequency(String value, int freq) {
    this.value = value;
    this.freq = freq;
  }

  public int freq() {
    return freq;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(value);
    out.writeInt(freq);
  }

  public static ValueFrequency read(DataInput in) throws IOException {
    final String value = in.readUTF();
    final int freq = in.readInt();
    return new ValueFrequency(value, freq);
  }

  @Override
  public String toString() {
    return String.format("(%s:%d)", value, freq);
  }

  public long estimateSize() {
    final long size = SizeEstimator.estimatedSizeOf("") + SizeEstimator.estimatedSizeOf(value) + 4;
    return size;
  }
}
