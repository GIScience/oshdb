package org.heigit.ohsome.oshdb.api.db;

import java.nio.file.Path;

public class H2Support {

  private H2Support() {
  }

  public static String pathToUrl(Path path) {
    var absolutePath = path.toAbsolutePath().toString();
    absolutePath = absolutePath.replaceAll("\\.mv\\.db$", "");
    return String.format("jdbc:h2:%s;ACCESS_MODE_DATA=r", absolutePath);
  }
}
