package de.dbathon.jds.resource;

import static de.dbathon.jds.util.JsonUtil.readJsonString;
import static de.dbathon.jds.util.JsonUtil.readObjectFromJsonBytes;
import static de.dbathon.jds.util.JsonUtil.toJsonStreamingOutputPretty;

import java.util.List;

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
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import de.dbathon.jds.rest.ApiException;
import de.dbathon.jds.rest.RestHelper;
import de.dbathon.jds.service.DatabaseService;
import de.dbathon.jds.service.DocumentService;
import de.dbathon.jds.util.JsonMap;

@Path("{databaseName:" + DatabaseService.NAME_PATTERN_STRING + "}")
@ApplicationScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DocumentResource {

  @Inject
  private RestHelper restHelper;
  @Inject
  private DocumentService documentService;

  @GET
  @Path("{documentId:" + DocumentService.ID_PATTERN_STRING + "}")
  public StreamingOutput get(@PathParam("databaseName") final String databaseName,
      @PathParam("documentId") final String documentId) {
    return toJsonStreamingOutputPretty(documentService.getDocument(databaseName, documentId));
  }

  @PUT
  @Path("{documentId:" + DocumentService.ID_PATTERN_STRING + "}")
  public Response put(@PathParam("databaseName") final String databaseName,
      @PathParam("documentId") final String documentId, final byte[] jsonBytes, @Context final UriInfo uriInfo) {
    final JsonMap json = readObjectFromJsonBytes(jsonBytes);
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
    return restHelper.buildJsonResponse(response, new JsonMap().add("id", documentId).add("version", newVersion));
  }

  @DELETE
  @Path("{documentId:" + DocumentService.ID_PATTERN_STRING + "}")
  public StreamingOutput delete(@PathParam("databaseName") final String databaseName,
      @PathParam("documentId") final String documentId, @QueryParam("version") final String version) {
    if (version == null) {
      throw new ApiException("version parameter is missing");
    }
    documentService.deleteDocument(databaseName, documentId, version);
    return toJsonStreamingOutputPretty(new JsonMap());
  }

  // TODO PATCH

  private Integer tryParseInteger(final String string, final String name) {
    if (string == null || string.isEmpty()) {
      return null;
    }
    try {
      return Integer.parseInt(string);
    }
    catch (final NumberFormatException e) {
      throw new ApiException("invalid " + name, e);
    }
  }

  @GET
  @Path("_query")
  public Response query(@PathParam("databaseName") final String databaseName,
      @QueryParam("filters") final String filters, @QueryParam("limit") final String limit,
      @QueryParam("offset") final String offset) {
    final List<JsonMap> documents =
        documentService.queryDocuments(databaseName, filters != null ? readJsonString(filters) : null,
            tryParseInteger(limit, "limit"), tryParseInteger(offset, "offset"));
    return restHelper.buildResultResponse(Status.OK, documents);
  }

  @GET
  @Path("_count")
  public Response count(@PathParam("databaseName") final String databaseName,
      @QueryParam("filters") final String filters) {
    final Long count = documentService.countDocuments(databaseName, filters != null ? readJsonString(filters) : null);
    return restHelper.buildResultResponse(Status.OK, count);
  }

}
