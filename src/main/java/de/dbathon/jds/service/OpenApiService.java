package de.dbathon.jds.service;

import static de.dbathon.jds.util.JsonUtil.array;
import static de.dbathon.jds.util.JsonUtil.object;
import static java.util.Objects.requireNonNull;

import javax.enterprise.context.ApplicationScoped;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.core.UriInfo;

@ApplicationScoped
public class OpenApiService {

  private JsonArrayBuilder buildServers(final UriInfo uriInfo) {
    final JsonArrayBuilder servers = array();
    servers.add(object().add("url", uriInfo.getBaseUri().toString()));
    return servers;
  }

  private JsonObjectBuilder buildInfo(final String databaseName) {
    final JsonObjectBuilder info = object();
    info.add("description", "description...");
    info.add("version", "0.1");
    info.add("title", databaseName == null ? "jds - json datastore" : "database " + databaseName + " in jds");
    info.add("license", object().add("name", "MIT").add("url", "https://opensource.org/licenses/MIT"));
    return info;
  }

  private JsonArrayBuilder buildTags(final String databaseName) {
    final JsonArrayBuilder tags = array();
    if (databaseName == null) {
      tags.add(object().add("name", "database"));
    }
    tags.add(object().add("name", "document"));
    if (databaseName == null) {
      tags.add(object().add("name", "api"));
    }
    return tags;
  }

  private void addPathParameters(final JsonArrayBuilder parameters, final String... parameterNames) {
    for (final String parameterName : parameterNames) {
      final boolean queryParam = parameterName.startsWith("&");
      final JsonObjectBuilder parameter = object();
      parameter.add("name", parameterName.substring(queryParam ? 1 : 0));
      parameter.add("in", queryParam ? "query" : "path");
      parameter.add("required", true);
      parameter.add("schema", object().add("type", "string"));
      parameters.add(parameter);
    }
  }

  private JsonObjectBuilder basicOperation(final String summary, final String tag, final String... parameterNames) {
    final JsonObjectBuilder operation = object();
    operation.add("summary", summary);
    operation.add("tags", array().add(tag));
    if (parameterNames.length > 0) {
      final JsonArrayBuilder parameters = array();
      addPathParameters(parameters, parameterNames);
      operation.add("parameters", parameters);
    }
    return operation;
  }

  private JsonObjectBuilder addJsonRequestBody(final JsonObjectBuilder operation) {
    operation.add("requestBody", object().add("content",
        object().add("application/json", object().add("schema", object().add("type", "object")))));
    return operation;
  }

  private JsonObjectBuilder addJsonResponse(final JsonObjectBuilder operation) {
    operation.add("responses", object().add("200", object().add("content",
        object().add("application/json", object().add("schema", object().add("type", "object"))))));
    return operation;
  }

  private JsonObjectBuilder databaseOperation(final String summary) {
    return basicOperation(summary, "database", "databaseName");
  }

  private JsonObjectBuilder buildDatabasePath() {
    final JsonObjectBuilder path = object();
    path.add("get", addJsonResponse(databaseOperation("get information about the database")));
    path.add("put", addJsonResponse(addJsonRequestBody(databaseOperation("create or update a database"))));
    path.add("delete", addJsonResponse(basicOperation("delete a database", "database", "databaseName", "&version")));
    return path;
  }

  private JsonObjectBuilder documentOperation(final String databaseName, final String summary) {
    return databaseName == null ? basicOperation(summary, "document", "databaseName", "documentId")
        : basicOperation(summary, "document", "documentId");
  }

  private JsonObjectBuilder buildDocumentPath(final String databaseName) {
    final JsonObjectBuilder path = object();
    path.add("get", addJsonResponse(documentOperation(databaseName, "get a document")));
    path.add("put",
        addJsonResponse(addJsonRequestBody(documentOperation(databaseName, "create or update a document"))));
    path.add("delete",
        addJsonResponse(databaseName == null
            ? basicOperation("delete a document", "document", "databaseName", "documentId", "&version")
            : basicOperation("delete a document", "document", "documentId", "&version")));
    return path;
  }

  private JsonObjectBuilder buildOpenApiPath(final String... parameterNames) {
    final JsonObjectBuilder path = object();
    path.add("get", addJsonResponse(basicOperation("get the OpenApi specification", "api", parameterNames)));
    return path;
  }

  private JsonObjectBuilder buildPaths(final String databaseName) {
    final JsonObjectBuilder paths = object();
    if (databaseName == null) {
      paths.add("/{databaseName}", buildDatabasePath());
      paths.add("/{databaseName}/{documentId}", buildDocumentPath(null));
      paths.add("/_openApi.json", buildOpenApiPath());
      paths.add("/{databaseName}/_openApi.json", buildOpenApiPath("databaseName"));
    }
    else {
      paths.add("/" + databaseName + "/{documentId}", buildDocumentPath(databaseName));
    }
    return paths;
  }

  private JsonObject buildSpecification(final UriInfo uriInfo, final String databaseName) {
    final JsonObjectBuilder spec = object();
    spec.add("openapi", "3.0.0");
    spec.add("servers", buildServers(uriInfo));
    spec.add("info", buildInfo(databaseName));
    spec.add("tags", buildTags(databaseName));
    spec.add("paths", buildPaths(databaseName));
    return spec.build();
  }

  public JsonObject buildSpecification(final UriInfo uriInfo) {
    return buildSpecification(uriInfo, null);
  }

  public JsonObject buildDatabaseSpecificSpecification(final UriInfo uriInfo, final String databaseName) {
    return buildSpecification(uriInfo, requireNonNull(databaseName));
  }

}
