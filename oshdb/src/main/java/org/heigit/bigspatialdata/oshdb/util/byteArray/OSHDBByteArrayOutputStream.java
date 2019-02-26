package org.heigit.bigspatialdata.oshdb.util.byteArray;

import java.io.ByteArrayOutputStream;

public class OSHDBByteArrayOutputStream extends ByteArrayOutputStream {
  
  public OSHDBByteArrayOutputStream(int size) {
    super(size);
  }
  
  public byte[] array() {
    return buf;
  }
  
  public int length() {
    return count;
  }
}
