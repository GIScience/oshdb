package org.heigit.bigspatialdata.oshpbf.parser.pbf;

public class PosContainer<T> {
  public final long pos;
  public final T content;

  public PosContainer(long pos, T content) {
    this.pos = pos;
    this.content = content;
  }

  @Override
  public String toString() {
    return String.format("Pos:%d - %s", pos, content.toString());
  }
  
  public static <T> PosContainer<T>  get(long pos, T content){
    return new PosContainer<>(pos, content);
  }
  
}
