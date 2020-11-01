package de.dbathon.jds.service;

import static de.dbathon.jds.util.JsonUtil.readJsonString;
import static de.dbathon.jds.util.JsonUtil.toJsonString;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.ws.rs.core.Response.Status;

import de.dbathon.jds.persistence.DatabaseConnection;
import de.dbathon.jds.persistence.RuntimeSqlException;
import de.dbathon.jds.service.DatabaseService.DatabaseInfo;
import de.dbathon.jds.util.JsonMap;
import de.dbathon.jds.util.JsonUtil;

@ApplicationScoped
@Transactional
public class DocumentService {

  public static final Pattern ID_PATTERN = DatabaseService.NAME_PATTERN;

  public static final String ID_PROPERTY = "id";
  public static final String VERSION_PROPERTY = "version";
  public static final List<String> SPECIAL_STRING_PROPERTIES =
      Collections.unmodifiableList(Arrays.asList(ID_PROPERTY, VERSION_PROPERTY));

  private static final Class<?>[] STRING_STRING_TYPES = new Class<?>[] { String.class, String.class };
  private static final Class<?>[] STRING_STRING_STRING_TYPES =
      new Class<?>[] { String.class, String.class, String.class };

  @Inject
  DatabaseConnection databaseConnection;

  @Inject
  DatabaseCache databaseCache;

  public static class DocumentInfo implements Serializable {
    public final DatabaseInfo databaseInfo;
    public final String id;
    /**
     * The document version, which is the version of the database at which the document was inserted
     * or last updated.
     * <p>
     * See {@link DatabaseInfo#version}.
     */
    public final String version;

    public DocumentInfo(final DatabaseInfo databaseInfo, final String id, final String version) {
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
    final String version = databaseConnection.queryNoOrOneResult(
        "select version from jds_document where database_id = ? and id = ?", String.class, databaseInfo.id, documentId);
    if (version == null) {
      throw notFoundException();
    }
    return new DocumentInfo(databaseInfo, documentId, version);
  }

  public static void validateId(final String documentId) {
    if (!ID_PATTERN.matcher(documentId).matches()) {
      throw new ApiException("invalid document id: " + documentId).withDocumentId(documentId);
    }
  }

  private JsonMap buildJsonObject(final String id, final String version, final String dataJson) {
    final JsonMap result = new JsonMap().add(ID_PROPERTY, id).add(VERSION_PROPERTY, version);
    final JsonMap data = (JsonMap) JsonUtil.readJsonString(dataJson);
    result.putAll(data);
    return result;
  }

  public JsonMap getDocument(final String databaseName, final String documentId) {
    // no locking and only one select
    final Object[] row = databaseConnection.queryNoOrOneResult(
        "select d.version, d.data from jds_document d "
            + "join jds_database b on d.database_id = b.id where b.name = ? and d.id = ?",
        Object[].class, STRING_STRING_TYPES, databaseName, documentId);
    if (row == null) {
      throw notFoundException();
    }
    return buildJsonObject(documentId, (String) row[0], (String) row[1]);
  }

  private ApiException versionDoesNotMatchException() {
    return new ApiException("document version does not match", Status.CONFLICT);
  }

  private DocumentInfo getDocumentInfoAndLockAndCheckVersion(final String databaseName, final String documentId,
      final String version) {
    final DocumentInfo info = getDocumentInfoAndLock(databaseName, documentId);
    if (!version.equals(info.version)) {
      throw versionDoesNotMatchException();
    }
    return info;
  }

  private JsonMap validateAndRemoveSpecialProperties(final JsonMap json, final String expectedId,
      final String expectedVersion) {
    final JsonMap result = new JsonMap();
    boolean versionSeen = false;

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
        // just keep everything else
        result.put(key, value);
      }
    }

    if (expectedVersion != null && !versionSeen) {
      throw new ApiException("version is missing");
    }

    return result;
  }

  private void extractReferencedIdsRecursive(final Object object, final Set<String> result) {
    if (object instanceof Map<?, ?>) {
      @SuppressWarnings("unchecked")
      final Map<String, ?> map = (Map<String, ?>) object;
      map.entrySet().forEach(entry -> {
        final String key = entry.getKey();
        final Object value = entry.getValue();

        if ((key.length() >= 3 && key.endsWith("Id"))
            || (key.length() >= 4 && (key.endsWith("_id") || key.endsWith("_ID")))) {
          if (value instanceof String) {
            result.add((String) value);
          }
          else if (value != null) {
            throw new ApiException("must be a string: " + key);
          }
        }
        else if ((key.length() >= 4 && key.endsWith("Ids"))
            || (key.length() >= 5 && (key.endsWith("_ids") || key.endsWith("_IDS")))) {
              if (value instanceof List<?>) {
                ((List<?>) value).forEach(element -> {
                  if (element instanceof String) {
                    result.add((String) element);
                  }
                  else {
                    throw new ApiException("must contain only strings: " + key);
                  }
                });
              }
              else if (value != null) {
                throw new ApiException("must be a list of strings: " + key);
              }
            }
        // if the key is "data" then don't search for ids in value
        else if (!"data".contentEquals(key)) {
          extractReferencedIdsRecursive(value, result);
        }
      });
    }
    else if (object instanceof List<?>) {
      ((List<?>) object).forEach(element -> {
        extractReferencedIdsRecursive(element, result);
      });
    }
  }

  private void updateReferences(final DatabaseInfo databaseInfo, final String documentId, final JsonMap json,
      final boolean isNew) {
    final Set<String> referencedIds = new HashSet<>();
    extractReferencedIdsRecursive(json, referencedIds);

    final Set<String> existingIds;
    if (isNew) {
      existingIds = Collections.emptySet();
    }
    else {
      // query the ids from the database
      existingIds = new HashSet<>(databaseConnection.query(
          "select to_document_id from jds_reference where database_id = ? and from_document_id = ?", String.class,
          databaseInfo.id, documentId));
    }

    if (!referencedIds.equals(existingIds)) {
      final Set<String> toAdd = new HashSet<>(referencedIds);
      toAdd.removeAll(existingIds);
      final Set<String> toDelete = new HashSet<>(existingIds);
      toDelete.removeAll(referencedIds);

      // TODO: optimize with batched inserts/deletes
      toAdd.forEach(id -> {
        try {
          databaseConnection.executeUpdate(
              "insert into jds_reference (database_id, from_document_id, to_document_id) values (?, ?, ?)",
              databaseInfo.id, documentId, id);
        }
        catch (final RuntimeSqlException e) {
          if (e.isIntegrityContraintViolation()) {
            // referenced document does not exist
            throw new ApiException("referenced document does not exist: " + id, e, Status.CONFLICT);
          }
          throw e;
        }
      });
      toDelete.forEach(id -> {
        databaseConnection.executeUpdate(
            "delete from jds_reference where database_id = ? and from_document_id = ? and to_document_id = ?",
            databaseInfo.id, documentId, id);
      });
    }
  }

  private <T> T withApiExceptionDocumentIdHandling(final String documentId, final Supplier<T> supplier) {
    try {
      return supplier.get();
    }
    catch (final ApiException e) {
      if (e.getDocumentId() == null) {
        e.withDocumentId(documentId);
      }
      throw e;
    }
  }

  public String createDocument(final String databaseName, final String documentId, final JsonMap json) {
    return withApiExceptionDocumentIdHandling(documentId, () -> {
      validateId(documentId);
      final DatabaseInfo databaseInfo = databaseCache.getDatabaseInfoAndLock(databaseName);
      final String dataJson = toJsonString(validateAndRemoveSpecialProperties(json, documentId, null));
      final String version = databaseCache.getIncrementedVersion(databaseInfo);
      try {
        final int insertCount = databaseConnection.executeUpdate(
            "insert into jds_document (database_id, id, version, data) values (?, ?, ?, ?::jsonb)", databaseInfo.id,
            documentId, version, dataJson);
        if (insertCount != 1) {
          throw new IllegalStateException("unexpected insertCount: " + insertCount);
        }
      }
      catch (final RuntimeSqlException e) {
        if (e.isIntegrityContraintViolation()) {
          // document already exists
          throw new ApiException("document already exists", e, Status.CONFLICT);
        }
        throw e;
      }
      updateReferences(databaseInfo, documentId, json, true);
      return version;
    });
  }

  public String updateDocument(final String databaseName, final String documentId, final JsonMap json) {
    return withApiExceptionDocumentIdHandling(documentId, () -> {
      validateId(documentId);
      final DocumentInfo info = getDocumentInfoAndLock(databaseName, documentId);
      final JsonMap processedJson = validateAndRemoveSpecialProperties(json, documentId, info.version);

      // load the existing document and compare to see if the document is unchanged
      final JsonMap existingJson = (JsonMap) readJsonString(databaseConnection.queryNoOrOneResult(
          "select data from jds_document where database_id = ? and id = ? and version = ?", String.class,
          info.databaseInfo.id, documentId, info.version));
      if (existingJson.equals(processedJson)) {
        // no changes, don't update
        return info.version;
      }

      final String dataJson = toJsonString(processedJson);
      final String newVersion = databaseCache.getIncrementedVersion(info.databaseInfo);

      final int updateCount = databaseConnection.executeUpdate(
          "update jds_document set version = ?, data = ?::jsonb where database_id = ? and id = ? and version = ?",
          newVersion, dataJson, info.databaseInfo.id, documentId, info.version);
      if (updateCount != 1) {
        // the update must work, since we locked above
        throw new IllegalStateException("update failed unexpectedly: " + updateCount);
      }
      updateReferences(info.databaseInfo, documentId, json, false);
      return newVersion;
    });
  }

  public void deleteDocument(final String databaseName, final String documentId, final String version) {
    withApiExceptionDocumentIdHandling(documentId, () -> {
      final DocumentInfo info = getDocumentInfoAndLockAndCheckVersion(databaseName, documentId, version);
      try {
        // first delete potentially existing outgoing references
        databaseConnection.executeUpdate("delete from jds_reference where database_id = ? and from_document_id = ?",
            info.databaseInfo.id, info.id);

        // then delete the document
        final int updateCount = databaseConnection
            .executeUpdate("delete from jds_document where database_id = ? and id = ?", info.databaseInfo.id, info.id);
        if (updateCount != 1) {
          // the delete must work, since we locked above
          throw new IllegalStateException("delete failed unexpectedly: " + updateCount);
        }
        return null;
      }
      catch (final RuntimeSqlException e) {
        if (e.isIntegrityContraintViolation()) {
          // database name already exists
          throw new ApiException("document is referenced", e, Status.CONFLICT);
        }
        throw e;
      }
    });
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
      throw new ApiException("invalid operand for contains: " + toJsonString(value));
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

  private List<?> toList(final Iterable<?> filters) {
    if (filters instanceof List<?>) {
      // simple case, just cast
      return (List<?>) filters;
    }
    final List<Object> result = new ArrayList<>();
    filters.forEach(result::add);
    return result;
  }

  private void applyFilters(final QueryBuilder queryBuilder, final Object filters) {
    if (filters instanceof Map<?, ?>) {
      for (final Entry<?, ?> entry : ((Map<?, ?>) filters).entrySet()) {
        // keys must be strings
        final String key = String.valueOf(entry.getKey());
        final Object value = entry.getValue();

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
    else if (filters instanceof Iterable<?>) {
      final List<?> list = toList((Iterable<?>) filters);
      if (!list.isEmpty()) {
        final String operator;
        final List<?> restList;
        if (list.get(0) instanceof String) {
          operator = (String) list.get(0);
          restList = list.subList(1, list.size());
        }
        else {
          // default to and
          operator = "and";
          restList = list;
        }

        final Consumer<Runnable> queryBuilderWith;
        Consumer<Object> specialEntryAction = null;

        switch (operator) {
        case "contains":
          // for contains, we need a different entryAction
          specialEntryAction = entry -> applyContainsOperator(queryBuilder, entry);
          // fall through
        case "and":
          queryBuilderWith = queryBuilder::withAnd;
          break;
        case "not":
          queryBuilderWith = queryBuilder::withNot;
          break;
        case "or":
          queryBuilderWith = queryBuilder::withOr;
          break;
        default:
          throw new ApiException("unexpected operator: " + operator);
        }

        final Consumer<Object> entryAction =
            specialEntryAction != null ? specialEntryAction : entry -> applyFilters(queryBuilder, entry);

        queryBuilderWith.accept(() -> {
          restList.forEach(entryAction);
        });
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
        STRING_STRING_STRING_TYPES, queryBuilder.getParametersArray());

    final List<JsonMap> result = new ArrayList<>();
    for (final Object[] row : rows) {
      result.add(buildJsonObject((String) row[0], (String) row[1], (String) row[2]));
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
