package de.dbathon.jds.resource;

import static de.dbathon.jds.util.JsonUtil.object;
import static de.dbathon.jds.util.JsonUtil.toBytesPretty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.JsonObject;
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
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import de.dbathon.jds.rest.ApiException;
import de.dbathon.jds.rest.RestHelper;
import de.dbathon.jds.service.DatabaseService;
import de.dbathon.jds.service.DocumentService;

@Path("{databaseName:" + DatabaseService.NAME_PATTERN_STRING + "}/{documentId:" + DocumentService.ID_PATTERN_STRING
    + "}")
@ApplicationScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DocumentResource {

  @Inject
  private RestHelper restHelper;
  @Inject
  private DocumentService documentService;

  @GET
  public byte[] get(@PathParam("databaseName") final String databaseName,
      @PathParam("documentId") final String documentId) {
    return toBytesPretty(documentService.getDocument(databaseName, documentId));
  }

  @PUT
  public Response put(@PathParam("databaseName") final String databaseName,
      @PathParam("documentId") final String documentId, final JsonObject json, @Context final UriInfo uriInfo) {
    final String newVersion;
    final ResponseBuilder response;
    if (!json.containsKey("version")) {
      // create
      newVersion = documentService.createDocument(databaseName, documentId, json);
      response = Response.created(uriInfo.getAbsolutePath());
    }
    else {
      // rename
      newVersion = documentService.updateDocument(databaseName, documentId, json);
      response = Response.ok();
    }
    // do not return the full document, just the id and version
    return restHelper.buildJsonResponse(response, object().add("id", documentId).add("version", newVersion).build());
  }

  @DELETE
  public byte[] delete(@PathParam("databaseName") final String databaseName,
      @PathParam("documentId") final String documentId, @QueryParam("version") final String version) {
    if (version == null) {
      throw new ApiException("version parameter is missing");
    }
    documentService.deleteDocument(databaseName, documentId, version);
    return toBytesPretty(object());
  }

  // TODO PATCH

}
