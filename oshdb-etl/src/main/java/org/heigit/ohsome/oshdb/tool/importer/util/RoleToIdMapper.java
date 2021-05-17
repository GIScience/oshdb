package org.heigit.ohsome.oshdb.tool.importer.util;

public interface RoleToIdMapper {

  int getRole(String role);

  long estimatedSize();
}
