package org.heigit.bigspatialdata.updater.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexHandler {
  private static final Logger LOG = LoggerFactory.getLogger(IndexHandler.class);

  public static void flag(File bitMap, long id) throws FileNotFoundException, IOException, ClassNotFoundException {
    LongBitmapDataProvider index;
    try {
      FileInputStream fis = new FileInputStream(bitMap);
      ObjectInputStream ois = new ObjectInputStream(fis);
      index = (LongBitmapDataProvider) ois.readObject();
    } catch (FileNotFoundException ex) {
      LOG.warn("File not found, creating", ex);
      bitMap.createNewFile();
      index = new Roaring64NavigableMap();
    }
    index.addLong(id);
    FileOutputStream fos = new FileOutputStream(bitMap);
    ObjectOutputStream oos = new ObjectOutputStream(fos);
    oos.writeObject(index);
    oos.close();
  }

  public static boolean isUpdated(File bitMap, long id) throws FileNotFoundException, IOException, ClassNotFoundException {
    FileInputStream fis = new FileInputStream(bitMap);
    ObjectInputStream ois = new ObjectInputStream(fis);
    LongBitmapDataProvider index = (LongBitmapDataProvider) ois.readObject();
    return index.contains(id);
  }

}
