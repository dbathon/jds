package de.dbathon.jds.resource;

import static de.dbathon.jds.util.JsonUtil.toJsonStreamingOutputPretty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import de.dbathon.jds.service.OpenApiService;

@Path("_openApi.json")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
public class OpenApiResource {

  @Inject
  private OpenApiService openApiService;

  @GET
  public StreamingOutput getOpenApiSpecification(@Context final UriInfo uriInfo) {
    return toJsonStreamingOutputPretty(openApiService.buildSpecification(uriInfo));
  }

}
