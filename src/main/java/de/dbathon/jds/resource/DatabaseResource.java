package de.dbathon.jds.resource;

import static de.dbathon.jds.util.JsonUtil.readObjectFromJsonBytes;
import static de.dbathon.jds.util.JsonUtil.toJsonStreamingOutputPretty;
import static java.util.Objects.requireNonNull;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import de.dbathon.jds.rest.ApiException;
import de.dbathon.jds.rest.RestHelper;
import de.dbathon.jds.service.DatabaseService;
import de.dbathon.jds.util.JsonMap;

@Path("{databaseName:" + DatabaseService.NAME_PATTERN_STRING + "}")
@ApplicationScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DatabaseResource {

  @Inject
  private DatabaseService databaseService;

  @Inject
  private RestHelper restHelper;

  @GET
  public StreamingOutput get(@PathParam("databaseName") final String databaseName) {
    return toJsonStreamingOutputPretty(databaseService.getDatabase(databaseName));
  }

  @PUT
  public Response put(@PathParam("databaseName") final String databaseName, final byte[] jsonBytes,
      @Context final UriInfo uriInfo) {
    final JsonMap json = readObjectFromJsonBytes(jsonBytes);
    final String name, version;
    try {
      name = requireNonNull((String) json.get("name"));
      version = (String) json.get("version");
    }
    catch (final RuntimeException e) {
      throw new ApiException("invalid name or version");
    }
    if (version == null) {
      // create
      if (!databaseName.equals(name)) {
        throw new ApiException("name does not match");
      }
      return restHelper.buildJsonResponse(Response.created(uriInfo.getAbsolutePath()),
          databaseService.createDatabase(name));
    }
    else {
      // rename
      return restHelper.buildJsonResponse(Status.OK, databaseService.renameDatabase(databaseName, version, name));
    }
  }

  @DELETE
  public StreamingOutput delete(@PathParam("databaseName") final String databaseName,
      @QueryParam("version") final String version) {
    if (version == null) {
      throw new ApiException("version parameter is missing");
    }
    databaseService.deleteDatabase(databaseName, version);
    return toJsonStreamingOutputPretty(new JsonMap());
  }

}
