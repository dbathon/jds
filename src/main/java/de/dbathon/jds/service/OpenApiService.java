package de.dbathon.jds.service;

import static java.util.Objects.requireNonNull;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.core.UriInfo;

import de.dbathon.jds.util.JsonList;
import de.dbathon.jds.util.JsonMap;

@ApplicationScoped
public class OpenApiService {

  private JsonList buildServers(final UriInfo uriInfo) {
    return new JsonList().addElement(new JsonMap().add("url", uriInfo.getBaseUri().toString()));
  }

  private JsonMap buildInfo(final String databaseName) {
    final JsonMap info = new JsonMap();
    info.add("description", "description...");
    info.add("version", "0.1");
    info.add("title", databaseName == null ? "jds - json datastore" : "database " + databaseName + " in jds");
    info.add("license", new JsonMap().add("name", "MIT").add("url", "https://opensource.org/licenses/MIT"));
    return info;
  }

  private JsonList buildTags(final String databaseName) {
    final JsonList tags = new JsonList();
    if (databaseName == null) {
      tags.add(new JsonMap().add("name", "database"));
    }
    tags.add(new JsonMap().add("name", "document"));
    if (databaseName == null) {
      tags.add(new JsonMap().add("name", "api"));
    }
    return tags;
  }

  private JsonMap addParameters(final JsonMap operation, final String... parameterNames) {
    if (parameterNames.length > 0) {
      JsonList parameters = (JsonList) operation.get("parameters");
      if (parameters == null) {
        parameters = new JsonList();
        operation.add("parameters", parameters);
      }
      for (final String parameterName : parameterNames) {
        String name = parameterName;
        final boolean queryParam = parameterName.startsWith("&");
        if (queryParam) {
          name = name.substring(1);
        }
        final boolean optional = parameterName.endsWith("?");
        if (optional) {
          name = name.substring(0, name.length() - 1);
        }
        final JsonMap parameter = new JsonMap();
        parameter.add("name", name);
        parameter.add("in", queryParam ? "query" : "path");
        parameter.add("required", !optional);
        parameter.add("schema", new JsonMap().add("type", "string"));
        parameters.add(parameter);
      }
    }
    return operation;
  }

  private JsonMap basicOperation(final String summary, final String tag, final String... parameterNames) {
    final JsonMap operation = new JsonMap();
    operation.add("summary", summary);
    operation.add("tags", new JsonList().addElement(tag));
    addParameters(operation, parameterNames);
    return operation;
  }

  private JsonMap addJsonRequestBody(final JsonMap operation) {
    operation.add("requestBody", new JsonMap().add("content",
        new JsonMap().add("application/json", new JsonMap().add("schema", new JsonMap().add("type", "object")))));
    return operation;
  }

  private JsonMap addJsonResponse(final JsonMap operation) {
    operation.add("responses", new JsonMap().add("200", new JsonMap().add("content",
        new JsonMap().add("application/json", new JsonMap().add("schema", new JsonMap().add("type", "object"))))));
    return operation;
  }

  private JsonMap addVersionParameter(final JsonMap operation) {
    addParameters(operation, "&version");
    return operation;
  }

  private JsonMap databaseOperation(final String summary) {
    return basicOperation(summary, "database", "databaseName");
  }

  private JsonMap buildDatabasePath() {
    final JsonMap path = new JsonMap();
    path.add("get", addJsonResponse(databaseOperation("get information about the database")));
    path.add("put", addJsonResponse(addJsonRequestBody(databaseOperation("create or update a database"))));
    path.add("delete", addJsonResponse(addVersionParameter(databaseOperation("delete a database"))));
    return path;
  }

  private JsonMap documentOperation(final String databaseName, final String summary) {
    return databaseName == null ? basicOperation(summary, "document", "databaseName", "documentId")
        : basicOperation(summary, "document", "documentId");
  }

  private JsonMap buildDocumentPath(final String databaseName) {
    final JsonMap path = new JsonMap();
    path.add("get", addJsonResponse(documentOperation(databaseName, "get a document")));
    path.add("put",
        addJsonResponse(addJsonRequestBody(documentOperation(databaseName, "create or update a document"))));
    path.add("delete", addJsonResponse(addVersionParameter(documentOperation(databaseName, "delete a document"))));
    return path;
  }

  private JsonMap queryOperation(final String databaseName, final String summary) {
    return databaseName == null ? basicOperation(summary, "document", "databaseName")
        : basicOperation(summary, "document");
  }

  private JsonMap buildQueryPath(final String databaseName) {
    final JsonMap path = new JsonMap();
    path.add("get", addParameters(addJsonResponse(queryOperation(databaseName, "query documents")), "&filters?",
        "&limit?", "&offset?"));
    return path;
  }

  private JsonMap buildCountPath(final String databaseName) {
    final JsonMap path = new JsonMap();
    path.add("get", addParameters(addJsonResponse(queryOperation(databaseName, "count documents")), "&filters?"));
    return path;
  }

  private JsonMap buildOpenApiPath(final String... parameterNames) {
    final JsonMap path = new JsonMap();
    path.add("get", addJsonResponse(basicOperation("get the OpenApi specification", "api", parameterNames)));
    return path;
  }

  private JsonMap buildPaths(final String databaseName) {
    final JsonMap paths = new JsonMap();
    final String databasePath = databaseName == null ? "/{databaseName}" : "/" + databaseName;
    if (databaseName == null) {
      paths.add(databasePath, buildDatabasePath());
    }
    paths.add(databasePath + "/{documentId}", buildDocumentPath(databaseName));
    paths.add(databasePath + "/_query", buildQueryPath(databaseName));
    paths.add(databasePath + "/_count", buildCountPath(databaseName));
    if (databaseName == null) {
      paths.add("/_openApi.json", buildOpenApiPath());
      paths.add(databasePath + "/_openApi.json", buildOpenApiPath("databaseName"));
    }
    return paths;
  }

  private JsonMap buildSpecification(final UriInfo uriInfo, final String databaseName) {
    final JsonMap spec = new JsonMap();
    spec.add("openapi", "3.0.0");
    spec.add("servers", buildServers(uriInfo));
    spec.add("info", buildInfo(databaseName));
    spec.add("tags", buildTags(databaseName));
    spec.add("paths", buildPaths(databaseName));
    return spec;
  }

  public JsonMap buildSpecification(final UriInfo uriInfo) {
    return buildSpecification(uriInfo, null);
  }

  public JsonMap buildDatabaseSpecificSpecification(final UriInfo uriInfo, final String databaseName) {
    return buildSpecification(uriInfo, requireNonNull(databaseName));
  }

}
