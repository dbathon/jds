package de.dbathon.jds.service;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.io.StringWriter;
import java.util.Map;
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

  private static final Class<?>[] LONG_STRING_TYPES = new Class<?>[] { Long.class, String.class };

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
    final Long version = databaseConnection.queryNoOrOneResult(
        "select version from jds_document where database_id = ? and id = ? for update", Long.class, databaseInfo.id,
        documentId);
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
    final JsonMap result = new JsonMap().add("id", id).add("version", DatabaseService.toVersionString(version));
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
      if ("id".equals(key)) {
        // id is optional in the json, but if given it must match
        if (!(value instanceof String)) {
          throw new ApiException("invalid id");
        }
        else if (!expectedId.equals(value)) {
          throw new ApiException("id does not match");
        }
      }
      else if ("version".equals(key)) {
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

}
