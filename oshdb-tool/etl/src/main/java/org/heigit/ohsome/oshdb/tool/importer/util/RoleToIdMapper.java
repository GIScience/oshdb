package org.heigit.ohsome.oshdb.tool.importer.util;

public interface RoleToIdMapper {
  
  public int getRole(String role);

  public long estimatedSize();
}
