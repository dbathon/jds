package de.dbathon.jds.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.json.stream.JsonGenerator;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import de.dbathon.jds.util.JsonMap;
import de.dbathon.jds.util.JsonUtil;

@Provider
public class JsonMapBodyWriter implements MessageBodyWriter<JsonMap> {

  @Override
  public boolean isWriteable(final Class<?> type, final Type genericType, final Annotation[] annotations,
      final MediaType mediaType) {
    return JsonMap.class.isAssignableFrom(type);
  }

  @Override
  public void writeTo(final JsonMap json, final Class<?> type, final Type genericType, final Annotation[] annotations,
      final MediaType mediaType, final MultivaluedMap<String, Object> httpHeaders, final OutputStream entityStream)
      throws IOException, WebApplicationException {
    final JsonGenerator generator = JsonUtil.PRETTY_GENERATOR_FACTORY.createGenerator(entityStream);
    JsonUtil.writeToGenerator(json, generator);
    generator.flush();
    // do not close the generator, because that would also close the entityStream
  }

}
