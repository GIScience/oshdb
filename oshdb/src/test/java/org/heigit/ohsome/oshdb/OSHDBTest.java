package org.heigit.ohsome.oshdb;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class OSHDBTest {

  @Test
  public void convertCoordinates() {
    long l = 1231234567L;

    double convertedDouble = OSHDB.coordinateToDouble(l);
    assertEquals(123.1234567, convertedDouble, 0.0);

    long convertedLong = OSHDB.coordinateToLong(convertedDouble);
    assertEquals(l, convertedLong);
  }

}
