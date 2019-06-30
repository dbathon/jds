package de.dbathon.jds.rest;

import static de.dbathon.jds.util.JsonUtil.readJsonString;
import static de.dbathon.jds.util.JsonUtil.readObjectFromJsonBytes;

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
import javax.ws.rs.core.UriInfo;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import de.dbathon.jds.service.ApiException;
import de.dbathon.jds.service.DocumentService;
import de.dbathon.jds.util.JsonMap;

@Path("{databaseName}")
@ApplicationScoped
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "document")
public class DocumentResource {

  @Inject
  DocumentService documentService;

  @GET
  @Path("{documentId}")
  @Operation(summary = "get a document")
  @APIResponse(responseCode = "200", content = @Content(schema = @Schema(ref = "jsonObject")))
  public JsonMap get(@PathParam("databaseName") final String databaseName,
      @PathParam("documentId") final String documentId) {
    return documentService.getDocument(databaseName, documentId);
  }

  @PUT
  @Path("{documentId}")
  @Operation(summary = "create or update a document")
  @APIResponse(responseCode = "200", content = @Content(schema = @Schema(ref = "jsonObject")))
  @APIResponse(responseCode = "201", content = @Content(schema = @Schema(ref = "jsonObject")))
  public Response put(@PathParam("databaseName") final String databaseName,
      @PathParam("documentId") final String documentId,
      @RequestBody(content = @Content(schema = @Schema(ref = "jsonObject"))) final byte[] jsonBytes,
      @Context final UriInfo uriInfo) {
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
    return RestUtil.buildJsonResponse(response, new JsonMap().add("id", documentId).add("version", newVersion));
  }

  @DELETE
  @Path("{documentId}")
  @Operation(summary = "delete a document")
  @APIResponse(responseCode = "200", content = @Content(schema = @Schema(ref = "jsonObject")))
  public JsonMap delete(@PathParam("databaseName") final String databaseName,
      @PathParam("documentId") final String documentId, @QueryParam("version") final String version) {
    if (version == null) {
      throw new ApiException("version parameter is missing");
    }
    documentService.deleteDocument(databaseName, documentId, version);
    return new JsonMap();
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
  @Operation(summary = "query documents")
  @APIResponse(responseCode = "200", content = @Content(schema = @Schema(ref = "jsonObject")))
  public Response query(
      @PathParam("databaseName") @Parameter(name = "databaseName", required = true) final String databaseName,
      @QueryParam("filters") @Parameter(name = "filters", required = false) final String filters,
      @QueryParam("limit") @Parameter(name = "limit", required = false,
          schema = @Schema(type = SchemaType.NUMBER)) final String limit,
      @QueryParam("offset") @Parameter(name = "offset", schema = @Schema(type = SchemaType.NUMBER),
          required = false) final String offset) {
    final List<JsonMap> documents =
        documentService.queryDocuments(databaseName, filters != null ? readJsonString(filters) : null,
            tryParseInteger(limit, "limit"), tryParseInteger(offset, "offset"));
    return RestUtil.buildResultResponse(Status.OK, documents);
  }

  @GET
  @Path("_count")
  @Operation(summary = "count documents")
  @APIResponse(responseCode = "200", content = @Content(schema = @Schema(ref = "jsonObject")))
  public Response count(
      @PathParam("databaseName") @Parameter(name = "databaseName", required = true) final String databaseName,
      @QueryParam("filters") @Parameter(name = "filters", required = false) final String filters) {
    final Long count = documentService.countDocuments(databaseName, filters != null ? readJsonString(filters) : null);
    return RestUtil.buildResultResponse(Status.OK, count);
  }

}
