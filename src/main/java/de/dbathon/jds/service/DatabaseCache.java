package de.dbathon.jds.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.transaction.TransactionScoped;

import de.dbathon.jds.service.DatabaseService.DatabaseInfo;

/**
 * Caches infos about databases for the current transaction.
 */
@TransactionScoped
public class DatabaseCache implements Serializable {

  @Inject
  DatabaseService databaseService;

  /**
   * Every database in this map has been locked in the current transaction.
   */
  private final Map<String, DatabaseInfo> nameToInfo = new HashMap<>();

  /**
   * Entries in here are potentially not locked.
   */
  private final Map<String, Integer> nameToId = new HashMap<>();

  private final Map<Integer, Long> idToIncrementedVersion = new HashMap<>();

  public DatabaseInfo getDatabaseInfoAndLock(final String databaseName) {
    DatabaseInfo result = nameToInfo.get(databaseName);
    if (result == null) {
      result = databaseService.getDatabaseInfoAndLock(databaseName);
      nameToInfo.put(databaseName, result);
    }
    return result;
  }

  public Integer getDatabaseId(final String databaseName) {
    // if we locked the database before, then use that
    final DatabaseInfo databaseInfo = nameToInfo.get(databaseName);
    if (databaseInfo != null) {
      return databaseInfo.id;
    }
    // otherwise check nameToId
    Integer result = nameToId.get(databaseName);
    if (result == null) {
      result = databaseService.getDatabaseId(databaseName);
      nameToId.put(databaseName, result);
    }
    return result;
  }

  /**
   * This method increments the version of the database once per tranasaction (for each database).
   * This method should only be called if the database is already locked and if updates to documents
   * in the database will be performed.
   *
   * @param databaseInfo
   * @return the incremented version for the database
   */
  public Long getIncrementedVersion(final DatabaseInfo databaseInfo) {
    Long result = idToIncrementedVersion.get(databaseInfo.id);
    if (result == null) {
      result = databaseService.incrementVersion(databaseInfo);
      idToIncrementedVersion.put(databaseInfo.id, result);
    }
    return result;
  }

}
