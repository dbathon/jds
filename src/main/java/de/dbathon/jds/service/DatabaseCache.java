package de.dbathon.jds.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.transaction.TransactionScoped;
import javax.transaction.Transactional;
import javax.transaction.Transactional.TxType;

import de.dbathon.jds.service.DatabaseService.DatabaseInfo;

/**
 * Caches infos about databases for the current transaction.
 */
@TransactionScoped
@Transactional(TxType.MANDATORY)
public class DatabaseCache implements Serializable {

  @Inject
  private DatabaseService databaseService;

  private final Map<String, DatabaseInfo> nameToInfo = new HashMap<>();
  private final Map<Integer, Long> idToIncrementedVersion = new HashMap<>();

  public DatabaseInfo getDatabaseInfoAndLock(final String databaseName) {
    DatabaseInfo result = nameToInfo.get(databaseName);
    if (result == null) {
      result = databaseService.getDatabaseInfoAndLock(databaseName);
      nameToInfo.put(databaseName, result);
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
