package de.dbathon.jds.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.stream.JsonParsingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@ApplicationScoped
public class AnyExceptionMapper implements ExceptionMapper<Throwable> {

  private static final Logger log = LoggerFactory.getLogger(AnyExceptionMapper.class);

  @Inject
  private RestHelper restHelper;

  private List<Throwable> reverseCauseList(Throwable throwable) {
    final List<Throwable> result = new ArrayList<>();

    // prevent infinite loop
    int depth = 0;

    while (throwable != null && ++depth <= 100) {
      result.add(throwable);
      throwable = throwable.getCause();
    }

    Collections.reverse(result);
    return result;
  }

  @Override
  public Response toResponse(final Throwable exception) {
    log.debug("request failed with exception: " + exception.getMessage() + ", " + exception.getClass());

    Response response = null;
    // try to find an "expected" exception from the inside out
    for (final Throwable e : reverseCauseList(exception)) {
      if (e instanceof WebApplicationException) {
        // no logging for these exceptions, just return the response
        return ((WebApplicationException) e).getResponse();
      }
      else if (e instanceof ApiException) {
        final ApiException requestError = (ApiException) e;
        // no logging
        return restHelper.buildErrorResponse(requestError.getStatus(), requestError.getMessage());
      }
      else if (e instanceof JsonParsingException) {
        response = restHelper.buildErrorResponse(Status.BAD_REQUEST, "invalid json");
      }
      // TODO: more?

      if (response != null) {
        log.info("request failed with expected exception", e);
        return response;
      }
    }

    // log original exception
    log.warn("request failed with unexpected exception", exception);
    return restHelper.buildErrorResponse(Status.INTERNAL_SERVER_ERROR, "unexpected error");
  }

}
