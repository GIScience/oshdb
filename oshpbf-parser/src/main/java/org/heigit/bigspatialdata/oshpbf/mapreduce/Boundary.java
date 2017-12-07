package org.heigit.bigspatialdata.oshpbf.mapreduce;

public class Boundary {

  /** Start position. */
  private final long start;

  /** End position. */
  private final long end;
  
  public Boundary(final long start, final long end){
    this.start = start;
    this.end = end;
  }

  /**
   * Gets boundary start position.
   *
   * @return Start position.
   */
  public long getStart() {
    return start;
  }

  /**
   * Gets boundary end position.
   *
   * @return Start position.
   */
  public long getEnd() {
    return end;
  }

  
  @Override
  public String toString() {
    return String.format("Boundary(start:%s,end:%s)", start,end);
  }
  
  
}
