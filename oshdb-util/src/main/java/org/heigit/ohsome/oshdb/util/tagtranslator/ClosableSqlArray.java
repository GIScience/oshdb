package org.heigit.ohsome.oshdb.util.tagtranslator;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

public class ClosableSqlArray implements AutoCloseable {

  public static <T> ClosableSqlArray createArray(Connection conn, String typeName, Collection<T> elements) throws SQLException {
    var array = conn.createArrayOf(typeName, elements.toArray());
    return new ClosableSqlArray(array);
  }

  private Array array;

  public ClosableSqlArray(Array array) {
    this.array = array;
  }

  public Array get() {
    return array;
  }

  @Override
  public void close() throws Exception {
    array.free();
  }
}
