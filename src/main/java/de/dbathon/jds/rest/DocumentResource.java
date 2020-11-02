package de.dbathon.jds.rest;

import static de.dbathon.jds.util.JsonUtil.readJsonString;
import static de.dbathon.jds.util.JsonUtil.readObjectFromJsonBytes;
import static de.dbathon.jds.util.JsonUtil.toJsonString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
import de.dbathon.jds.service.DocumentService.OperationType;
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
    if (!json.containsKey(DocumentService.VERSION_PROPERTY)) {
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
      throw new ApiException("version parameter is missing").withDocumentId(documentId);
    }
    documentService.deleteDocument(databaseName, documentId, version);
    return new JsonMap();
  }

  // TODO PATCH

  private static class IdAndVersionAndDocument {
    final String id;
    final String version;
    final JsonMap document;

    public IdAndVersionAndDocument(final String id, final String version, final JsonMap document) {
      this.id = id;
      this.version = version;
      this.document = document;
    }
  }

  private Stream<IdAndVersionAndDocument> getListElements(final JsonMap json, final String property) {
    final Object value = json.get(property);
    if (value == null) {
      return Stream.empty();
    }
    if (!(value instanceof List<?>)) {
      throw new ApiException(property + " is not an array");
    }
    return ((List<?>) value).stream().map(element -> {
      if (!(element instanceof JsonMap)) {
        throw new ApiException("all elements of " + property + " need to be objects");
      }
      final JsonMap document = (JsonMap) element;

      final Object id = document.get(DocumentService.ID_PROPERTY);
      if (!(id instanceof String)) {
        throw new ApiException("invalid " + DocumentService.ID_PROPERTY + ": " + toJsonString(id));
      }

      final Object version = document.get(DocumentService.VERSION_PROPERTY);
      if (!(version == null || version instanceof String)) {
        throw new ApiException("invalid " + DocumentService.VERSION_PROPERTY + ": " + toJsonString(version));
      }

      return new IdAndVersionAndDocument((String) id, (String) version, document);
    });
  }

  @POST
  @Path("_multi")
  @Operation(summary = "create, update or delete multiple documents in one request")
  @APIResponse(responseCode = "200", content = @Content(schema = @Schema(ref = "jsonObject")))
  public JsonMap multi(@PathParam("databaseName") final String databaseName,
      @RequestBody(content = @Content(schema = @Schema(ref = "jsonObject"))) final byte[] jsonBytes) {
    final JsonMap json = readObjectFromJsonBytes(jsonBytes);

    final List<DocumentService.Operation> operations = new ArrayList<>();
    // allow each id only once (there is no reason to allow multiple operations for one document)
    final Set<String> seenIds = new HashSet<>();
    getListElements(json, "put").forEach(entry -> {
      if (!seenIds.add(entry.id)) {
        throw new ApiException("only one change per document allowed");
      }

      operations.add(new DocumentService.Operation(entry.version == null ? OperationType.CREATE : OperationType.UPDATE,
          entry.id, entry.document, null));
    });

    getListElements(json, "delete").forEach(entry -> {
      if (!seenIds.add(entry.id)) {
        throw new ApiException("only one operation per document allowed");
      }
      if (entry.version == null) {
        throw new ApiException(DocumentService.VERSION_PROPERTY + " is required for delete");
      }

      operations.add(new DocumentService.Operation(OperationType.DELETE, entry.id, null, entry.version));
    });

    if (operations.isEmpty()) {
      throw new ApiException("no put or delete operations specified");
    }

    final Map<String, String> newVersions = documentService.performOperations(databaseName, operations);

    return new JsonMap().add("newDocumentVersions", newVersions);
  }

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
