package org.heigit.ohsome.oshdb.tool.importer.util.long2long.page;

public interface Page {
  long get(int offset);

  int weigh();
}
