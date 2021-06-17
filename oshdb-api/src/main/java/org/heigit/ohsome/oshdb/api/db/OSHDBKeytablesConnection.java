package org.heigit.ohsome.oshdb.api.db;

import java.sql.Connection;

/**
 * An interface for providing access to a keytables database.
 */
public interface OSHDBKeytablesConnection {
  Connection getConnection();
}
