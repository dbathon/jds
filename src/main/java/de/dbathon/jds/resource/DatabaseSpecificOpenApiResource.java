package de.dbathon.jds.resource;

import static de.dbathon.jds.util.JsonUtil.toBytesPretty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import de.dbathon.jds.service.DatabaseService;
import de.dbathon.jds.service.OpenApiService;

@Path("{databaseName:" + DatabaseService.NAME_PATTERN_STRING + "}/_openApi.json")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
public class DatabaseSpecificOpenApiResource {

  @Inject
  private OpenApiService openApiService;

  @GET
  public byte[] getOpenApiSpecification(@PathParam("databaseName") final String databaseName,
      @Context final UriInfo uriInfo) {
    return toBytesPretty(openApiService.buildDatabaseSpecificSpecification(uriInfo, databaseName));
  }

}
