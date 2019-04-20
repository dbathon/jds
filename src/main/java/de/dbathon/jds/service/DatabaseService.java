package de.dbathon.jds.service;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Random;
import java.util.regex.Pattern;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.ws.rs.core.Response.Status;

import de.dbathon.jds.persistence.DatabaseConnection;
import de.dbathon.jds.persistence.RuntimeSqlException;
import de.dbathon.jds.rest.ApiException;
import de.dbathon.jds.util.JsonMap;

@ApplicationScoped
@Transactional
public class DatabaseService {

  public static final String NAME_PATTERN_STRING = "[a-zA-Z0-9][a-zA-Z0-9_\\-]{0,199}";
  public static final Pattern NAME_PATTERN = Pattern.compile(NAME_PATTERN_STRING);

  private static final Class<?>[] INT_LONG_TYPES = new Class<?>[] { Integer.class, Long.class };

  @Inject
  private DatabaseConnection databaseConnection;

  public static class DatabaseInfo implements Serializable {
    public final Integer id;
    public final String name;
    public final Long version;

    public DatabaseInfo(final Integer id, final String name, final Long version) {
      this.id = requireNonNull(id);
      this.name = requireNonNull(name);
      this.version = requireNonNull(version);
    }
  }

  private ApiException notFoundException() {
    return new ApiException("database not found", Status.NOT_FOUND);
  }

  public DatabaseInfo getDatabaseInfoAndLock(final String databaseName) {
    final Object[] row = databaseConnection.queryNoOrOneResult(
        "select id, version from jds_database where name = ? for update", Object[].class, INT_LONG_TYPES, databaseName);
    if (row == null) {
      throw notFoundException();
    }
    return new DatabaseInfo((Integer) row[0], databaseName, (Long) row[1]);
  }

  /**
   * Represent the version as {@link String}, the client should consider the version as an opaque
   * string...
   */
  public static String toVersionString(final long version) {
    return String.valueOf(version);
  }

  private JsonMap databaseJson(final String name, final String version) {
    return new JsonMap().add("name", name).add("version", version);
  }

  public JsonMap getDatabase(final String databaseName) {
    final Long version = databaseConnection.queryNoOrOneResult("select version from jds_database where name = ?",
        Long.class, databaseName);
    if (version == null) {
      throw notFoundException();
    }
    return databaseJson(databaseName, toVersionString(version));
  }

  private void validateName(final String databaseName) {
    if (!NAME_PATTERN.matcher(databaseName).matches()) {
      throw new ApiException("invalid database name");
    }
  }

  public JsonMap createDatabase(final String databaseName) {
    validateName(databaseName);
    // try up to 100 times with random ids...
    final Random random = new Random();
    try {
      for (int i = 0; i < 100; ++i) {
        // random id between 1 and Integer.MAX_VALUE
        final int id = random.nextInt(Integer.MAX_VALUE) + 1;
        final int insertCount = databaseConnection.executeUpdate(
            "insert into jds_database (id, name, version) values (?, ?, 0) on conflict (id) do nothing", id,
            databaseName);
        if (insertCount == 1) {
          // success
          return databaseJson(databaseName, "0");
        }
      }
    }
    catch (final RuntimeSqlException e) {
      if (e.isIntegrityContraintViolation()) {
        // database name already exists
        throw new ApiException("database already exists", e, Status.CONFLICT);
      }
      throw e;
    }
    throw new RuntimeException("database create with random id failed too many times");
  }

  private DatabaseInfo getDatabaseInfoAndLockAndCheckVersion(final String databaseName, final String version) {
    final DatabaseInfo info = getDatabaseInfoAndLock(databaseName);
    if (!version.equals(toVersionString(info.version))) {
      throw new ApiException("version does not match", Status.CONFLICT);
    }
    return info;
  }

  private Long getNextVersion(final DatabaseInfo databaseInfo) {
    if (databaseInfo.version == Long.MAX_VALUE) {
      // TODO: better solution?
      throw new ApiException("too many database versions");
    }
    return databaseInfo.version + 1;
  }

  public JsonMap renameDatabase(final String oldDatabaseName, final String oldVersion, final String newDatabaseName) {
    validateName(newDatabaseName);
    final DatabaseInfo info = getDatabaseInfoAndLockAndCheckVersion(oldDatabaseName, oldVersion);
    if (oldDatabaseName.equals(newDatabaseName)) {
      // nohing to do
      return databaseJson(oldDatabaseName, oldVersion);
    }
    final Long newVersion = getNextVersion(info);
    try {
      final int updateCount =
          databaseConnection.executeUpdate("update jds_database set name = ?, version = ? where id = ? and version = ?",
              newDatabaseName, newVersion, info.id, info.version);
      if (updateCount != 1) {
        // the update must work, since we locked above
        throw new IllegalStateException("rename failed unexpectedly: " + updateCount);
      }
      return databaseJson(newDatabaseName, toVersionString(newVersion));
    }
    catch (final RuntimeSqlException e) {
      if (e.isIntegrityContraintViolation()) {
        // database name already exists
        throw new ApiException("database with name " + newDatabaseName + " already exists", e, Status.CONFLICT);
      }
      throw e;
    }
  }

  public void deleteDatabase(final String databaseName, final String version) {
    final DatabaseInfo info = getDatabaseInfoAndLockAndCheckVersion(databaseName, version);
    try {
      final int updateCount = databaseConnection.executeUpdate("delete from jds_database where id = ?", info.id);
      if (updateCount != 1) {
        // the delete must work, since we locked above
        throw new IllegalStateException("delete failed unexpectedly: " + updateCount);
      }
    }
    catch (final RuntimeSqlException e) {
      if (e.isIntegrityContraintViolation()) {
        // database name already exists
        throw new ApiException("database is not empty", e, Status.CONFLICT);
      }
      throw e;
    }
  }

  /**
   * The given {@link DatabaseInfo} should be locked and this method should only be called once per
   * transaction. Generally just use {@link DatabaseCache#getIncrementedVersion(DatabaseInfo)}.
   */
  public Long incrementVersion(final DatabaseInfo databaseInfo) {
    final Long newVersion = getNextVersion(databaseInfo);
    final int updateCount =
        databaseConnection.executeUpdate("update jds_database set version = ? where id = ? and version = ?", newVersion,
            databaseInfo.id, databaseInfo.version);
    if (updateCount != 1) {
      // the update must work, since we locked above
      throw new IllegalStateException("version increment failed unexpectedly: " + updateCount);
    }
    return newVersion;
  }

}
