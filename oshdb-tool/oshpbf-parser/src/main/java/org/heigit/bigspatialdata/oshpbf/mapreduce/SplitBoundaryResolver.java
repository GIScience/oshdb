package org.heigit.bigspatialdata.oshpbf.mapreduce;

import java.io.IOException;


public class SplitBoundaryResolver {

  /**
   * 
   * The PBF BlobHeader is a repeating sequence of: - int4: length of the BlobHeader message in
   * network byte order - serialized BlobHeader message + required string type = 1; + optional bytes
   * indexdata = 2; + required int32 datasize = 3;
   * 
   * http://wiki.openstreetmap.org/wiki/PBF_Format#File_format
   * 
   */
  private static final int BlobHeaderLength = 4;

  /**
   * 
   * Tag((field_number << 3) | wire_type => 10)+sizeOfString +"OSMData".getBytes();
   * 
   */
  private static final byte[] SIGNATURE_OSMDATA = {10, 7, 79, 83, 77, 68, 97, 116, 97};


  public Boundary resolve(RandomAccessInputStream splitInputStream, long suggestedStart, long suggestedEnd)
      throws IOException {
     
    splitInputStream.seek(suggestedStart);
    final long start = suggestedStart == 0? 0 :findSignature(splitInputStream);
    splitInputStream.seek(suggestedEnd);
    final long end = findSignature(splitInputStream);

    return new Boundary(start, (end >= 0) ? end : suggestedEnd);
  }

  private long findSignature(RandomAccessInputStream is) throws IOException {
    int nextByte = is.read();
    int val = 0;
    while (nextByte != -1) {
      if (SIGNATURE_OSMDATA[val] == nextByte) {
        if (val == SIGNATURE_OSMDATA.length - 1) {
          // Full OSMData SIGNATURE is found.
          return is.position() - SIGNATURE_OSMDATA.length - BlobHeaderLength;
        }

        val++;
      } else if (val != 0) {
        val = 0;
        if (SIGNATURE_OSMDATA[val] == nextByte) {
          val++;
        }
      }
      nextByte = is.read();
    }
    return -1;
  }


}
