package de.dbathon.jds.util;

import java.io.ByteArrayOutputStream;
import java.util.Collections;

import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.json.JsonWriterFactory;
import javax.json.spi.JsonProvider;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;

public class JsonUtil {

  public static final JsonProvider PROVIDER = JsonProvider.provider();

  public static final JsonWriterFactory PRETTY_WRITER_FACTORY =
      PROVIDER.createWriterFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));
  public static final JsonGeneratorFactory PRETTY_GENERATOR_FACTORY =
      PROVIDER.createGeneratorFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));

  public static JsonObjectBuilder object() {
    return PROVIDER.createObjectBuilder();
  }

  public static JsonArrayBuilder array() {
    return PROVIDER.createArrayBuilder();
  }

  public static byte[] toBytesPretty(final JsonValue jsonValue) {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PRETTY_WRITER_FACTORY.createWriter(outputStream).write(jsonValue);
    return outputStream.toByteArray();
  }

  public static byte[] toBytesPretty(final JsonObjectBuilder jsonObjectBuilder) {
    return toBytesPretty(jsonObjectBuilder.build());
  }

  public static byte[] toBytesPretty(final JsonArrayBuilder jsonArrayBuilder) {
    return toBytesPretty(jsonArrayBuilder.build());
  }

}
