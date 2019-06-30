package de.dbathon.jds.rest;

import static de.dbathon.jds.util.JsonUtil.readObjectFromJsonBytes;
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
import javax.ws.rs.core.UriInfo;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import de.dbathon.jds.service.ApiException;
import de.dbathon.jds.service.DatabaseService;
import de.dbathon.jds.util.JsonMap;

@Path("{databaseName}")
@ApplicationScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "database")
public class DatabaseResource {

  @Inject
  DatabaseService databaseService;

  @GET
  @Operation(summary = "get information about the database")
  @APIResponse(responseCode = "200", content = @Content(schema = @Schema(ref = "jsonObject")))
  public JsonMap get(@PathParam("databaseName") final String databaseName) {
    return databaseService.getDatabase(databaseName);
  }

  @PUT
  @Operation(summary = "create or update a database")
  @APIResponse(responseCode = "200", content = @Content(schema = @Schema(ref = "jsonObject")))
  @APIResponse(responseCode = "201", content = @Content(schema = @Schema(ref = "jsonObject")))
  public Response put(@PathParam("databaseName") final String databaseName,
      @RequestBody(content = @Content(schema = @Schema(ref = "jsonObject"))) final byte[] jsonBytes,
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
      return RestUtil.buildJsonResponse(Response.created(uriInfo.getAbsolutePath()),
          databaseService.createDatabase(name));
    }
    else {
      // rename
      return RestUtil.buildJsonResponse(Status.OK, databaseService.renameDatabase(databaseName, version, name));
    }
  }

  @DELETE
  @Operation(summary = "delete a database")
  @APIResponse(responseCode = "200", content = @Content(schema = @Schema(ref = "jsonObject")))
  public JsonMap delete(@PathParam("databaseName") final String databaseName,
      @QueryParam("version") final String version) {
    if (version == null) {
      throw new ApiException("version parameter is missing");
    }
    databaseService.deleteDatabase(databaseName, version);
    return new JsonMap();
  }

}
