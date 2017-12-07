package org.heigit.bigspatialdata.oshdb.util;

public class OSHDBKeytablesNotFoundException extends RuntimeException {
    public OSHDBKeytablesNotFoundException() {
        super("Keytables database not found, or db doesn't contain the required \"keytables\" tables. "
            + "Make sure you have specified the right keytables database, for example by calling `keytables()` when using the oshdb-api.");
    }
}
