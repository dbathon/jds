package de.dbathon.jds.util;

import java.math.BigDecimal;
import java.math.BigInteger;

import javax.json.JsonNumber;

/**
 * A {@link JsonNumber} implementation that avoids parsing the number if possible. If the numerical
 * value is not accessed then it might not be parsed/converted to a {@link BigDecimal}.
 * <p>
 * The given number string is also normalized, so that the scale of the corresponding
 * {@link BigDecimal} is as small as possible, but not negative.
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

  /**
   * @param value
   *          must be a valid json number string
   */
  public JsonStringNumber(final String value) {
    this.value = normalizeNumber(value);
  }

  private String normalizeNumber(final String value) {
    // we assume that value is a valid json number string...

    if (value.contains("e") || value.contains("E")) {
      // handle 'e' by just parsing the string now
      BigDecimal temp = new BigDecimal(value);
      if (temp.scale() != 0) {
        temp = temp.stripTrailingZeros();
        if (temp.scale() < 0) {
          temp = temp.setScale(0);
        }
      }
      // since we already created the BigDecimal, just keep it
      bigDecimalValue = temp;
      return temp.toString();
    }
    else if (value.contains(".") && (value.endsWith("0") || value.endsWith("."))) {
      // just remove the trailing zeros and potentially the '.' from the string
      int index = value.length() - 1;
      while (index >= 0 && value.charAt(index) == '0') {
        --index;
      }
      if (index >= 0 && value.charAt(index) == '.') {
        --index;
      }
      return value.substring(0, index + 1);
    }
    else {
      // just return the value as is
      return value;
    }
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
