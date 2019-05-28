package de.dbathon.jds.service;

import static de.dbathon.jds.util.JsonUtil.toJsonString;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.stream.JsonGenerator;
import javax.transaction.Transactional;
import javax.ws.rs.core.Response.Status;

import de.dbathon.jds.persistence.DatabaseConnection;
import de.dbathon.jds.persistence.RuntimeSqlException;
import de.dbathon.jds.rest.ApiException;
import de.dbathon.jds.service.DatabaseService.DatabaseInfo;
import de.dbathon.jds.util.JsonMap;
import de.dbathon.jds.util.JsonUtil;

@ApplicationScoped
@Transactional
public class DocumentService {

  public static final String ID_PATTERN_STRING = DatabaseService.NAME_PATTERN_STRING;
  public static final Pattern ID_PATTERN = DatabaseService.NAME_PATTERN;

  public static final String ID_PROPERTY = "id";
  public static final String VERSION_PROPERTY = "version";
  public static final List<String> SPECIAL_STRING_PROPERTIES =
      Collections.unmodifiableList(Arrays.asList(ID_PROPERTY, VERSION_PROPERTY));

  private static final Class<?>[] LONG_STRING_TYPES = new Class<?>[] { Long.class, String.class };
  private static final Class<?>[] STRING_LONG_STRING_TYPES = new Class<?>[] { String.class, Long.class, String.class };

  @Inject
  private DatabaseConnection databaseConnection;

  @Inject
  private DatabaseCache databaseCache;

  public static class DocumentInfo implements Serializable {
    public final DatabaseInfo databaseInfo;
    public final String id;
    public final Long version;

    public DocumentInfo(final DatabaseInfo databaseInfo, final String id, final Long version) {
      this.databaseInfo = requireNonNull(databaseInfo);
      this.id = requireNonNull(id);
      this.version = requireNonNull(version);
    }
  }

  private ApiException notFoundException() {
    return new ApiException("document not found", Status.NOT_FOUND);
  }

  public DocumentInfo getDocumentInfoAndLock(final String databaseName, final String documentId) {
    final DatabaseInfo databaseInfo = databaseCache.getDatabaseInfoAndLock(databaseName);
    // no need to lock the specific document, since we are locking the whole database
    final Long version = databaseConnection.queryNoOrOneResult(
        "select version from jds_document where database_id = ? and id = ?", Long.class, databaseInfo.id, documentId);
    if (version == null) {
      throw notFoundException();
    }
    return new DocumentInfo(databaseInfo, documentId, version);
  }

  private void validateId(final String documentId) {
    if (!ID_PATTERN.matcher(documentId).matches()) {
      throw new ApiException("invalid document id");
    }
  }

  private JsonMap buildJsonObject(final String id, final Long version, final String dataJson) {
    final JsonMap result =
        new JsonMap().add(ID_PROPERTY, id).add(VERSION_PROPERTY, DatabaseService.toVersionString(version));
    final JsonMap data = (JsonMap) JsonUtil.readJsonString(dataJson);
    result.putAll(data);
    return result;
  }

  public JsonMap getDocument(final String databaseName, final String documentId) {
    // no locking and only one select
    final Object[] row = databaseConnection.queryNoOrOneResult(
        "select d.version, d.data from jds_document d "
            + "join jds_database b on d.database_id = b.id where b.name = ? and d.id = ?",
        Object[].class, LONG_STRING_TYPES, databaseName, documentId);
    if (row == null) {
      throw notFoundException();
    }
    return buildJsonObject(documentId, (Long) row[0], (String) row[1]);
  }

  private ApiException versionDoesNotMatchException() {
    return new ApiException("document version does not match", Status.CONFLICT);
  }

  private DocumentInfo getDocumentInfoAndLockAndCheckVersion(final String databaseName, final String documentId,
      final String version) {
    final DocumentInfo info = getDocumentInfoAndLock(databaseName, documentId);
    if (!version.equals(DatabaseService.toVersionString(info.version))) {
      throw versionDoesNotMatchException();
    }
    return info;
  }

  private String toDataJsonString(final JsonMap json, final String expectedId, final String expectedVersion) {
    final StringWriter stringWriter = new StringWriter();
    final JsonGenerator generator = JsonUtil.PROVIDER.createGenerator(stringWriter);
    boolean versionSeen = false;

    generator.writeStartObject();
    for (final Map.Entry<String, ?> entry : json.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();
      if (ID_PROPERTY.equals(key)) {
        // id is optional in the json, but if given it must match
        if (!(value instanceof String)) {
          throw new ApiException("invalid id");
        }
        else if (!expectedId.equals(value)) {
          throw new ApiException("id does not match");
        }
      }
      else if (VERSION_PROPERTY.equals(key)) {
        versionSeen = true;
        if (expectedVersion == null) {
          // should not happen
          throw new IllegalArgumentException("version in json, but expectedVersion is null");
        }
        else if (!(value instanceof String)) {
          throw new ApiException("invalid version");
        }
        else if (expectedVersion != null && !expectedVersion.equals(value)) {
          throw versionDoesNotMatchException();
        }
      }
      else {
        // just write everything else
        generator.writeKey(key);
        JsonUtil.writeToGenerator(value, generator);
      }
    }
    generator.writeEnd();
    generator.flush();

    if (expectedVersion != null && !versionSeen) {
      throw new ApiException("version is missing");
    }

    return stringWriter.toString();
  }

  public String createDocument(final String databaseName, final String documentId, final JsonMap json) {
    validateId(documentId);
    final DatabaseInfo databaseInfo = databaseCache.getDatabaseInfoAndLock(databaseName);
    final String dataJson = toDataJsonString(json, documentId, null);
    final Long version = databaseCache.getIncrementedVersion(databaseInfo);
    try {
      // random id between 1 and Integer.MAX_VALUE
      final int insertCount = databaseConnection.executeUpdate(
          "insert into jds_document (database_id, id, version, data) values (?, ?, ?, ?::jsonb)", databaseInfo.id,
          documentId, version, dataJson);
      if (insertCount != 1) {
        throw new IllegalStateException("unexpected insertCount: " + insertCount);
      }
      return DatabaseService.toVersionString(version);
    }
    catch (final RuntimeSqlException e) {
      if (e.isIntegrityContraintViolation()) {
        // database name already exists
        throw new ApiException("document already exists", e, Status.CONFLICT);
      }
      throw e;
    }
  }

  public String updateDocument(final String databaseName, final String documentId, final JsonMap json) {
    validateId(documentId);
    final DocumentInfo info = getDocumentInfoAndLock(databaseName, documentId);
    final String dataJson = toDataJsonString(json, documentId, DatabaseService.toVersionString(info.version));

    // TODO: detect unchanged document and don't increment version?

    final Long newVersion = databaseCache.getIncrementedVersion(info.databaseInfo);

    final int updateCount = databaseConnection.executeUpdate(
        "update jds_document set version = ?, data = ?::jsonb where database_id = ? and id = ? and version = ?",
        newVersion, dataJson, info.databaseInfo.id, documentId, info.version);
    if (updateCount != 1) {
      // the update must work, since we locked above
      throw new IllegalStateException("update failed unexpectedly: " + updateCount);
    }
    return DatabaseService.toVersionString(newVersion);
  }

  public void deleteDocument(final String databaseName, final String documentId, final String version) {
    final DocumentInfo info = getDocumentInfoAndLockAndCheckVersion(databaseName, documentId, version);
    try {
      final int updateCount = databaseConnection
          .executeUpdate("delete from jds_document where database_id = ? and id = ?", info.databaseInfo.id, info.id);
      if (updateCount != 1) {
        // the delete must work, since we locked above
        throw new IllegalStateException("delete failed unexpectedly: " + updateCount);
      }
    }
    catch (final RuntimeSqlException e) {
      if (e.isIntegrityContraintViolation()) {
        // database name already exists
        throw new ApiException("document is referenced", e, Status.CONFLICT);
      }
      throw e;
    }
  }

  private void applyFilterOperator(final QueryBuilder queryBuilder, final String key, final String operatorName,
      final Object rightHandSide) {
    final FilterOperator operator = FilterOperator.FILTER_OPERATORS.get(operatorName);
    if (operator == null) {
      throw new ApiException("unknown operator: " + operatorName);
    }
    operator.apply(queryBuilder, key, rightHandSide);
  }

  private void applyContainsOperator(final QueryBuilder queryBuilder, final Object value) {
    if (!(value instanceof JsonMap)) {
      throw new ApiException("invalid right hand side for _contains: " + toJsonString(value));
    }
    // copy the map, because we might modify it below
    final JsonMap map = new JsonMap((JsonMap) value);
    queryBuilder.withAnd(() -> {
      // handle special keys
      for (final String property : SPECIAL_STRING_PROPERTIES) {
        if (map.containsKey(property)) {
          final Object idValue = map.remove(property);
          if (idValue instanceof String) {
            applyFilterOperator(queryBuilder, property, "=", idValue);
          }
          else {
            // the value does not match
            queryBuilder.add("false");
          }
        }
      }

      queryBuilder.add("data @> ?::jsonb", toJsonString(map));
    });
  }

  private void applyFilters(final QueryBuilder queryBuilder, final Object filters) {
    if (filters instanceof Map<?, ?>) {
      for (final Entry<?, ?> entry : ((Map<?, ?>) filters).entrySet()) {
        // keys must be strings
        final String key = String.valueOf(entry.getKey());
        final Object value = entry.getValue();
        if (key.startsWith("_")) {
          // special cases
          switch (key) {
          case "_and":
            queryBuilder.withAnd(() -> {
              applyFilters(queryBuilder, value);
            });
            break;
          case "_or":
            queryBuilder.withOr(() -> {
              applyFilters(queryBuilder, value);
            });
            break;
          case "_contains":
            // special case where the key is the operator
            applyContainsOperator(queryBuilder, value);
            break;
          default:
            throw new ApiException("unexpected filter key: " + key);
          }
        }
        else {
          if (value instanceof Map<?, ?>) {
            // if it is a map then the key is the operator and the value is the right hand side
            for (final Entry<?, ?> operatorEntry : ((Map<?, ?>) value).entrySet()) {
              applyFilterOperator(queryBuilder, key, String.valueOf(operatorEntry.getKey()), operatorEntry.getValue());
            }
          }
          else {
            // the default operator is =
            applyFilterOperator(queryBuilder, key, "=", value);
          }
        }
      }
    }
    else if (filters instanceof Iterable<?>) {
      // for iterables just apply all the elements
      for (final Object element : (Iterable<?>) filters) {
        applyFilters(queryBuilder, element);
      }
    }
    else if (filters == null) {
      // just ignore null
    }
    else {
      throw new ApiException("invalid filters: " + toJsonString(filters));
    }
  }

  public List<JsonMap> queryDocuments(final String databaseName, final Object filters, final Integer limit,
      final Integer offset) {
    final Integer databaseId = databaseCache.getDatabaseId(databaseName);

    final QueryBuilder queryBuilder = new QueryBuilder();
    queryBuilder.add("select id, version, data from jds_document where");
    queryBuilder.withAnd(() -> {
      queryBuilder.add("database_id = ?", databaseId);

      applyFilters(queryBuilder, filters);
    });

    // use both columns of the primary key index, so that that index should be used
    queryBuilder.add("order by database_id, id");

    Integer effectiveLimit;
    if (limit == null) {
      // default to 100
      effectiveLimit = 100;
    }
    else {
      if (limit < 0) {
        throw new ApiException("invalid limit");
      }
      else if (limit > 1000) {
        throw new ApiException("limit too high");
      }
      else {
        effectiveLimit = limit;
      }
    }
    queryBuilder.add("limit ?", effectiveLimit);

    if (offset != null) {
      if (offset < 0) {
        throw new ApiException("invalid offset");
      }
      queryBuilder.add("offset ?", offset);
    }

    final List<Object[]> rows = databaseConnection.query(queryBuilder.getString(), Object[].class,
        STRING_LONG_STRING_TYPES, queryBuilder.getParametersArray());

    final List<JsonMap> result = new ArrayList<>();
    for (final Object[] row : rows) {
      result.add(buildJsonObject((String) row[0], (Long) row[1], (String) row[2]));
    }
    return result;
  }

  public Long countDocuments(final String databaseName, final Object filters) {
    final Integer databaseId = databaseCache.getDatabaseId(databaseName);

    final QueryBuilder queryBuilder = new QueryBuilder();
    queryBuilder.add("select count(*) from jds_document where");
    queryBuilder.withAnd(() -> {
      queryBuilder.add("database_id = ?", databaseId);

      applyFilters(queryBuilder, filters);
    });

    return databaseConnection.queryNoOrOneResult(queryBuilder.getString(), Long.class,
        queryBuilder.getParametersArray());
  }

}
