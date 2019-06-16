package de.dbathon.jds.rest;

import java.io.IOException;

import javax.ws.rs.Path;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;

import de.dbathon.jds.service.DatabaseService;
import de.dbathon.jds.service.DocumentService;

/**
 * This filter validates databaseName and documentId in the request path.
 * <p>
 * Using this filter (compared to using the pattern in {@link Path}) has the advantage that the user
 * gets a proper error, instead of just a 404. Also smallrye-openapi does not seem to handle
 * patterns in {@link Path} well...
 */
@Provider
public class PathValidationFilter implements ContainerRequestFilter {

  @Override
  public void filter(final ContainerRequestContext requestContext) throws IOException {
    final MultivaluedMap<String, String> pathParameters = requestContext.getUriInfo().getPathParameters();
    final String databaseName = pathParameters.getFirst("databaseName");
    if (databaseName != null) {
      DatabaseService.validateName(databaseName);
    }
    final String documentId = pathParameters.getFirst("documentId");
    if (documentId != null) {
      DocumentService.validateId(documentId);
    }
  }

}
