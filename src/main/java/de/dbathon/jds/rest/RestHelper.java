package de.dbathon.jds.rest;

import static de.dbathon.jds.util.JsonUtil.object;
import static de.dbathon.jds.util.JsonUtil.toBytesPretty;

import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.StatusType;

@ApplicationScoped
public class RestHelper {

  public Response buildJsonResponse(final ResponseBuilder builder, final JsonObject json) {
    builder.entity(toBytesPretty(json));
    builder.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    return builder.build();
  }

  public Response buildJsonResponse(final StatusType status, final JsonObject json) {
    return buildJsonResponse(Response.status(status), json);
  }

  public Response buildResultResponse(final StatusType status, final JsonValue json) {
    return buildJsonResponse(status, object().add("result", json).build());
  }

  public Response buildErrorResponse(final StatusType status, final String message) {
    return buildJsonResponse(status, object().add("error", message).build());
  }

}
