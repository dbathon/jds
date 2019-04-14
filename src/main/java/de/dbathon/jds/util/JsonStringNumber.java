package de.dbathon.jds.util;

import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.math.BigInteger;

import javax.json.JsonNumber;

/**
 * A {@link JsonNumber} implementation that avoids parsing the number if possible. If the numerical
 * value is not accessed then it will not be parsed.
 */
public class JsonStringNumber implements JsonNumber {

  /**
   * The unparsed number.
   */
  private final String value;

  /**
   * Lazily initialized.
   */
  private BigDecimal bigDecimalValue;

  public JsonStringNumber(final String value) {
    this.value = requireNonNull(value);
  }

  @Override
  public ValueType getValueType() {
    return ValueType.NUMBER;
  }

  @Override
  public String toString() {
    // just return the original string
    return value;
  }

  @Override
  public BigDecimal bigDecimalValue() {
    BigDecimal result = bigDecimalValue;
    if (result == null) {
      result = bigDecimalValue = new BigDecimal(value);
    }
    return result;
  }

  @Override
  public boolean isIntegral() {
    return bigDecimalValue().scale() == 0;
  }

  @Override
  public int intValue() {
    return bigDecimalValue().intValue();
  }

  @Override
  public int intValueExact() {
    return bigDecimalValue().intValueExact();
  }

  @Override
  public long longValue() {
    return bigDecimalValue().longValue();
  }

  @Override
  public long longValueExact() {
    return bigDecimalValue().longValueExact();
  }

  @Override
  public double doubleValue() {
    return bigDecimalValue().doubleValue();
  }

  @Override
  public BigInteger bigIntegerValue() {
    return bigDecimalValue().toBigInteger();
  }

  @Override
  public BigInteger bigIntegerValueExact() {
    return bigDecimalValue().toBigIntegerExact();
  }

  @Override
  public int hashCode() {
    return bigDecimalValue().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof JsonNumber)) {
      return false;
    }
    final JsonNumber other = (JsonNumber) obj;
    return bigDecimalValue().equals(other.bigDecimalValue());
  }

}
