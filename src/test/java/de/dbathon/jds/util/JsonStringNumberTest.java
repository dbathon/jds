package de.dbathon.jds.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

class JsonStringNumberTest {

  @Test
  void testNormalize() {
    assertEquals(new BigDecimal("1"), new JsonStringNumber("1").bigDecimalValue());
    assertEquals(new BigDecimal("1"), new JsonStringNumber("1.000").bigDecimalValue());
    assertEquals(new BigDecimal("1"), new JsonStringNumber("0.01000e2").bigDecimalValue());

    assertEquals(new BigDecimal("1"), new JsonStringNumber("0.01e2").bigDecimalValue());
    assertEquals(new BigDecimal("1"), new JsonStringNumber("1000e-3").bigDecimalValue());
    assertEquals(new BigDecimal("1"), new JsonStringNumber("0.000000001e9").bigDecimalValue());
    assertEquals(new BigDecimal("1"), new JsonStringNumber("100000000000e-11").bigDecimalValue());
    assertEquals(new BigDecimal("1000"), new JsonStringNumber("1e3").bigDecimalValue());

    assertEquals(new BigDecimal("1"), new JsonStringNumber("0.01E2").bigDecimalValue());
    assertEquals(new BigDecimal("1"), new JsonStringNumber("1000E-3").bigDecimalValue());
    assertEquals(new BigDecimal("1"), new JsonStringNumber("0.000000001E9").bigDecimalValue());
    assertEquals(new BigDecimal("1"), new JsonStringNumber("100000000000E-11").bigDecimalValue());
    assertEquals(new BigDecimal("1000"), new JsonStringNumber("1E3").bigDecimalValue());

    assertEquals(new BigDecimal("12.34"), new JsonStringNumber("12.34").bigDecimalValue());
    assertEquals(new BigDecimal("12.34"), new JsonStringNumber("12.34000").bigDecimalValue());
    assertEquals(new BigDecimal("12"), new JsonStringNumber("12.00000").bigDecimalValue());
    assertEquals(new BigDecimal("12"), new JsonStringNumber("12.0").bigDecimalValue());
    assertEquals(new BigDecimal("12"), new JsonStringNumber("12.").bigDecimalValue());
    assertEquals(new BigDecimal("12"), new JsonStringNumber("12").bigDecimalValue());
    assertEquals(new BigDecimal("10"), new JsonStringNumber("10.000").bigDecimalValue());
    assertEquals(new BigDecimal("10"), new JsonStringNumber("10.0").bigDecimalValue());
    assertEquals(new BigDecimal("10"), new JsonStringNumber("10.").bigDecimalValue());
  }

}
