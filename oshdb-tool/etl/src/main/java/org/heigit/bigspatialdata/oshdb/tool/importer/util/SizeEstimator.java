package org.heigit.bigspatialdata.oshdb.tool.importer.util;

public class SizeEstimator {
  
  private static int OBJ_HEADER;
  private static int ARR_HEADER;
  private static int PADDING;
  private static int INT_FIELD = 4;
  private static int OBJ_REF;
  private static int OBJ_OVERHEAD;
  private static boolean IS_64_BIT_JVM;

  private static final long AVL_ENTRY_SIZE = 96;
  private static final long AVL_MAP_SIZE = 128;
  
  static {
    // By default we assume 64 bit JVM
    // (defensive approach since we will get
    // larger estimations in case we are not sure)
    IS_64_BIT_JVM = true;
    // check the system property "sun.arch.data.model"
    // not very safe, as it might not work for all JVM implementations
    // nevertheless the worst thing that might happen is that the JVM is
    // 32bit
    // but we assume its 64bit, so we will be counting a few extra bytes per
    // string object
    // no harm done here since this is just an approximation.
    String arch = System.getProperty("sun.arch.data.model");
    if (arch != null) {
      if (arch.contains("32")) {
        // If exists and is 32 bit then we assume a 32bit JVM
        IS_64_BIT_JVM = false;
      }
    }
    // The sizes below are a bit rough as we don't take into account
    // advanced JVM options such as compressed oops
    // however if our calculation is not accurate it'll be a bit over
    // so there is no danger of an out of memory error because of this.
    OBJ_HEADER = IS_64_BIT_JVM ? 16 : 8;
    ARR_HEADER = IS_64_BIT_JVM ? 24 : 12;
    OBJ_REF = IS_64_BIT_JVM ? 8 : 4;
    PADDING = 4;
    OBJ_OVERHEAD = OBJ_HEADER + OBJ_REF + INT_FIELD + ARR_HEADER + PADDING;
  }
  
  public static long estimatedSizeOf(String s) {
    return (s.length() * 2) + OBJ_OVERHEAD;
  }
  
  public static long objOverhead(){
    return OBJ_OVERHEAD;
  }
  
  public static long intField(){
    return INT_FIELD;
  }
  
  public static long linkedListEntry(){
    return OBJ_HEADER+2*OBJ_REF;
  }
  
  public static long estimatedSizeOf(byte[] bytes) {
    return ARR_HEADER+bytes.length;
  }

  public static long linkedList() {
    return OBJ_HEADER+3*OBJ_REF + INT_FIELD;
  }

  public static long avlTreeEntry() {
    return AVL_ENTRY_SIZE;
  }
  
  public static long estimatedSizeOfAVLEntryKey(String key) {
    return estimatedSizeOf(key) + AVL_ENTRY_SIZE + AVL_MAP_SIZE;
  }

  public static long estimatedSizeOfAVLEntryValue(String value) {
    return estimatedSizeOf(value) + AVL_ENTRY_SIZE;
  }

  public static long estimateAvailableMemory() {
    System.gc();
    // http://stackoverflow.com/questions/12807797/java-get-available-memory
    Runtime r = Runtime.getRuntime();
    long allocatedMemory = r.totalMemory() - r.freeMemory();
    long presFreeMemory = r.maxMemory() - allocatedMemory;
    return presFreeMemory;
  }
  
}
