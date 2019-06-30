package de.dbathon.jds.util;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import javax.json.JsonValue;
import javax.json.spi.JsonProvider;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import javax.json.stream.JsonParsingException;

public class JsonUtil {

  public static final JsonProvider PROVIDER = JsonProvider.provider();

  public static final JsonGeneratorFactory PRETTY_GENERATOR_FACTORY =
      PROVIDER.createGeneratorFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));

  /**
   * Unfortunately the default pretty {@link JsonGenerator} implementation inserts extra newlines if
   * {@link JsonGenerator#writeKey(String)} is used, so to avoid that we have to use both variants
   * of all the write methods and lots of if/else...
   */
  private static void writeToGenerator(final Object value, final String outerKey, final JsonGenerator generator) {
    if (value == null) {
      if (outerKey == null) {
        generator.writeNull();
      }
      else {
        generator.writeNull(outerKey);
      }
    }
    else if (value instanceof String) {
      if (outerKey == null) {
        generator.write((String) value);
      }
      else {
        generator.write(outerKey, (String) value);
      }
    }
    else if (value instanceof Boolean) {
      if (outerKey == null) {
        generator.write((Boolean) value);
      }
      else {
        generator.write(outerKey, (Boolean) value);
      }
    }
    else if (value instanceof Number) {
      if (value instanceof BigDecimal) {
        if (outerKey == null) {
          generator.write((BigDecimal) value);
        }
        else {
          generator.write(outerKey, (BigDecimal) value);
        }
      }
      else if (value instanceof Integer) {
        if (outerKey == null) {
          generator.write((Integer) value);
        }
        else {
          generator.write(outerKey, (Integer) value);
        }
      }
      else if (value instanceof Long) {
        if (outerKey == null) {
          generator.write((Long) value);
        }
        else {
          generator.write(outerKey, (Long) value);
        }
      }
      else if (value instanceof BigInteger) {
        if (outerKey == null) {
          generator.write((BigInteger) value);
        }
        else {
          generator.write(outerKey, (BigInteger) value);
        }
      }
      else if (value instanceof Double) {
        if (outerKey == null) {
          generator.write((Double) value);
        }
        else {
          generator.write(outerKey, (Double) value);
        }
      }
      else if (value instanceof Float) {
        if (outerKey == null) {
          generator.write((Float) value);
        }
        else {
          generator.write(outerKey, (Float) value);
        }
      }
      else {
        throw new IllegalArgumentException("unsupported number type: " + value);
      }
    }
    else if (value instanceof JsonValue) {
      if (outerKey == null) {
        generator.write((JsonValue) value);
      }
      else {
        generator.write(outerKey, (JsonValue) value);
      }
    }
    else if (value instanceof Map<?, ?>) {
      if (outerKey == null) {
        generator.writeStartObject();
      }
      else {
        generator.writeStartObject(outerKey);
      }
      for (final Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        final Object key = entry.getKey();
        if (key instanceof String) {
          writeToGenerator(entry.getValue(), (String) key, generator);
        }
        else {
          throw new IllegalArgumentException("map keys must be strings: " + key);
        }
      }
      generator.writeEnd();
    }
    else if (value instanceof Iterable<?>) {
      if (outerKey == null) {
        generator.writeStartArray();
      }
      else {
        generator.writeStartArray(outerKey);
      }
      for (final Object element : (Iterable<?>) value) {
        writeToGenerator(element, null, generator);
      }
      generator.writeEnd();
    }
    else {
      throw new IllegalArgumentException("unsupported type: " + value);
    }
  }

  /**
   * Writes the given <code>value</code> to the given {@link JsonGenerator}.
   * <p>
   * The following types are supported: <code>null</code>, {@link String}, {@link Boolean},
   * {@link BigDecimal}, {@link Integer}, {@link Long}, {@link BigInteger}, {@link Double},
   * {@link Float}, {@link JsonValue}, {@link Map} (will be written as object) and {@link Iterable}
   * (will be written as array).
   */
  public static void writeToGenerator(final Object value, final JsonGenerator generator) {
    writeToGenerator(value, null, generator);
  }

  private static String toJsonString(final Object value, final boolean pretty) {
    final StringWriter writer = new StringWriter();
    try (final JsonGenerator generator =
        pretty ? PRETTY_GENERATOR_FACTORY.createGenerator(writer) : PROVIDER.createGenerator(writer)) {
      writeToGenerator(value, generator);
    }
    return writer.toString();
  }

  public static String toJsonString(final Object value) {
    return toJsonString(value, false);
  }

  public static String toJsonStringPretty(final Object value) {
    return toJsonString(value, true);
  }

  private static Object readFromParser(final JsonParser parser, final Event currentEvent) {
    if (currentEvent == null && !parser.hasNext()) {
      throw new JsonParsingException("unexpected end of input", parser.getLocation());
    }
    Event event = currentEvent != null ? currentEvent : parser.next();
    switch (event) {
    case START_OBJECT:
      final JsonMap map = new JsonMap();
      while ((event = parser.next()) != Event.END_OBJECT) {
        if (event != Event.KEY_NAME) {
          throw new JsonParsingException("unexpected event, expected KEY_NAME: " + event, parser.getLocation());
        }
        final String key = parser.getString();
        map.put(key, readFromParser(parser, null));
      }
      return map;
    case START_ARRAY:
      final JsonList list = new JsonList();
      while ((event = parser.next()) != Event.END_ARRAY) {
        list.add(readFromParser(parser, event));
      }
      return list;
    case VALUE_STRING:
      return parser.getString();
    case VALUE_NUMBER:
      return new JsonStringNumber(parser.getString());
    case VALUE_TRUE:
      return true;
    case VALUE_FALSE:
      return false;
    case VALUE_NULL:
      return null;
    case END_ARRAY:
    case END_OBJECT:
    case KEY_NAME:
    default:
      throw new JsonParsingException("unexpected event: " + event, parser.getLocation());
    }
  }

  /**
   * Builds an object tree by reading one "element" from the parser, the tree will consist of
   * instances of {@link JsonMap}, {@link JsonList}, {@link String}, {@link JsonStringNumber},
   * {@link Boolean} and <code>null</code>.
   */
  public static Object readFromParser(final JsonParser parser) {
    return readFromParser(parser, null);
  }

  public static Object readJsonString(final String json) {
    try (final JsonParser parser = PROVIDER.createParser(new StringReader(json))) {
      final Object result = readFromParser(parser);
      if (parser.hasNext()) {
        throw new JsonParsingException("unexpected extra input", parser.getLocation());
      }
      return result;
    }
  }

  public static Object readJsonBytes(final byte[] json) {
    try (final JsonParser parser = PROVIDER.createParser(new ByteArrayInputStream(json))) {
      final Object result = readFromParser(parser);
      if (parser.hasNext()) {
        throw new JsonParsingException("unexpected extra input", parser.getLocation());
      }
      return result;
    }
  }

  public static JsonMap readObjectFromJsonBytes(final byte[] json) {
    final Object result = readJsonBytes(json);
    if (!(result instanceof JsonMap)) {
      throw new JsonParsingException("not an object", null);
    }
    return (JsonMap) result;
  }

}
