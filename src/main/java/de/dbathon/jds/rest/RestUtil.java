package de.dbathon.jds.rest;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.StatusType;

import de.dbathon.jds.util.JsonMap;

public class RestUtil {

  public static Response buildJsonResponse(final ResponseBuilder builder, final JsonMap json) {
    builder.entity(json);
    builder.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    return builder.build();
  }

  public static Response buildJsonResponse(final StatusType status, final JsonMap json) {
    return buildJsonResponse(Response.status(status), json);
  }

  public static Response buildResultResponse(final StatusType status, final Object json) {
    return buildJsonResponse(status, new JsonMap().add("result", json));
  }

  public static Response buildErrorResponse(final StatusType status, final String message) {
    return buildJsonResponse(status, new JsonMap().add("error", message));
  }

}
