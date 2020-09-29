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
import de.dbathon.jds.util.JsonMap;

@ApplicationScoped
@Transactional
public class DatabaseService {

  public static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_\\-]{0,199}");

  static final String INITIAL_VERSION = "10";

  private static final Class<?>[] INT_STRING_TYPES = new Class<?>[] { Integer.class, String.class };

  @Inject
  DatabaseConnection databaseConnection;

  public static class DatabaseInfo implements Serializable {
    public final Integer id;
    public final String name;
    /**
     * The version of the database. This is a base 36 encoded number (1 to 9 and a to z) with a one
     * character long length prefix (also base 36). So 10, 11, 12, ..., 19, 1a, 1b, ..., 1z, 210,
     * 211, ...
     * <p>
     * The length prefix exists so that the versions can be compared lexicographically.
     * <p>
     * We could potentially also use base 62 (1 to 9, A to Z and a to z) or similar, but that could
     * lead to problems depending on the collation used when comparing, e.g. the collation might
     * order the letters like aAbB... instead of ABC...Zabc.... So using base 36 should just work in
     * most cases.
     */
    public final String version;

    public DatabaseInfo(final Integer id, final String name, final String version) {
      this.id = requireNonNull(id);
      this.name = requireNonNull(name);
      this.version = requireNonNull(version);
    }
  }

  private ApiException notFoundException() {
    return new ApiException("database not found", Status.NOT_FOUND);
  }

  public Integer getDatabaseId(final String databaseName) {
    final Integer id = databaseConnection.queryNoOrOneResult("select id from jds_database where name = ?",
        Integer.class, databaseName);
    if (id == null) {
      throw notFoundException();
    }
    return id;
  }

  public DatabaseInfo getDatabaseInfoAndLock(final String databaseName) {
    final Object[] row =
        databaseConnection.queryNoOrOneResult("select id, version from jds_database where name = ? for update",
            Object[].class, INT_STRING_TYPES, databaseName);
    if (row == null) {
      throw notFoundException();
    }
    return new DatabaseInfo((Integer) row[0], databaseName, (String) row[1]);
  }

  private JsonMap databaseJson(final String name, final String version) {
    return new JsonMap().add("name", name).add("version", version);
  }

  public JsonMap getDatabase(final String databaseName) {
    final String version = databaseConnection.queryNoOrOneResult("select version from jds_database where name = ?",
        String.class, databaseName);
    if (version == null) {
      throw notFoundException();
    }
    return databaseJson(databaseName, version);
  }

  public static void validateName(final String databaseName) {
    if (!NAME_PATTERN.matcher(databaseName).matches()) {
      throw new ApiException("invalid database name: " + databaseName);
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
            "insert into jds_database (id, name, version) values (?, ?, ?) on conflict (id) do nothing", id,
            databaseName, INITIAL_VERSION);
        if (insertCount == 1) {
          // success
          return databaseJson(databaseName, INITIAL_VERSION);
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
    if (!version.equals(info.version)) {
      throw new ApiException("version does not match", Status.CONFLICT);
    }
    return info;
  }

  private static char incrementChar(final char c) {
    if ((c >= 'a' && c < 'z') || (c >= '0' && c < '9')) {
      return (char) (c + 1);
    }
    if (c == '9') {
      return 'a';
    }
    if (c == 'z') {
      return '0';
    }
    throw new IllegalArgumentException("unexpected char: " + c);
  }

  /**
   * Increments the given <code>version</code> by one, see {@link DatabaseInfo#version}. This method
   * assumes that the <code>version</code> string is a valid version, it does not validate/parse it
   * completely.
   *
   * @param version
   * @return the incremented version
   */
  static String incrementVersionString(final String version) {
    if (version.length() < 2) {
      throw new IllegalArgumentException("version length needs to be at least 2");
    }
    // we don't validate all characters, only those that need to be changed...
    final char[] chars = version.toCharArray();
    for (int i = chars.length - 1; i > 0; --i) {
      final char incrementedChar = incrementChar(chars[i]);
      chars[i] = incrementedChar;
      if (incrementedChar != '0') {
        // done
        return new String(chars);
      }
    }

    // we need to increase the length
    final char[] newChars = new char[chars.length + 1];
    final char lengthIncremented = incrementChar(chars[0]);
    if (lengthIncremented == '0') {
      throw new IllegalArgumentException("cannot be incremented anymore: " + version);
    }
    newChars[0] = lengthIncremented;
    newChars[1] = '1';
    System.arraycopy(chars, 1, newChars, 2, chars.length - 1);

    return new String(newChars);
  }

  public JsonMap renameDatabase(final String oldDatabaseName, final String oldVersion, final String newDatabaseName) {
    validateName(newDatabaseName);
    final DatabaseInfo info = getDatabaseInfoAndLockAndCheckVersion(oldDatabaseName, oldVersion);
    if (oldDatabaseName.equals(newDatabaseName)) {
      // nothing to do
      return databaseJson(oldDatabaseName, oldVersion);
    }
    final String newVersion = incrementVersionString(info.version);
    try {
      final int updateCount =
          databaseConnection.executeUpdate("update jds_database set name = ?, version = ? where id = ? and version = ?",
              newDatabaseName, newVersion, info.id, info.version);
      if (updateCount != 1) {
        // the update must work, since we locked above
        throw new IllegalStateException("rename failed unexpectedly: " + updateCount);
      }
      return databaseJson(newDatabaseName, newVersion);
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
  public String incrementVersion(final DatabaseInfo databaseInfo) {
    final String newVersion = incrementVersionString(databaseInfo.version);
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
