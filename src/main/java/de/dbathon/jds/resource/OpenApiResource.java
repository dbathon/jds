package de.dbathon.jds.resource;

import static de.dbathon.jds.util.JsonUtil.toJsonBytesPretty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import de.dbathon.jds.service.OpenApiService;

@Path("_openApi.json")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
public class OpenApiResource {

  @Inject
  private OpenApiService openApiService;

  @GET
  public byte[] getOpenApiSpecification(@Context final UriInfo uriInfo) {
    return toJsonBytesPretty(openApiService.buildSpecification(uriInfo));
  }

}
