package de.dbathon.jds.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.TransactionSynchronizationRegistry;
import javax.transaction.Transactional;
import javax.transaction.Transactional.TxType;

import de.dbathon.jds.service.DatabaseService.DatabaseInfo;

/**
 * Caches infos about databases for the current transaction.
 */
@ApplicationScoped
@Transactional(TxType.MANDATORY)
public class DatabaseCache implements Serializable {

  @Inject
  DatabaseService databaseService;

  @Inject
  TransactionSynchronizationRegistry transactionSynchronizationRegistry;

  private static class State {
    /**
     * Every database in this map has been locked in the current transaction.
     */
    final Map<String, DatabaseInfo> nameToInfo = new HashMap<>();

    /**
     * Entries in here are potentially not locked.
     */
    final Map<String, Integer> nameToId = new HashMap<>();

    final Map<Integer, Long> idToIncrementedVersion = new HashMap<>();
  }

  private State getState() {
    State result = (State) transactionSynchronizationRegistry.getResource(State.class);
    if (result == null) {
      result = new State();
      transactionSynchronizationRegistry.putResource(State.class, result);
    }
    return result;
  }

  public DatabaseInfo getDatabaseInfoAndLock(final String databaseName) {
    final State state = getState();
    DatabaseInfo result = state.nameToInfo.get(databaseName);
    if (result == null) {
      result = databaseService.getDatabaseInfoAndLock(databaseName);
      state.nameToInfo.put(databaseName, result);
    }
    return result;
  }

  public Integer getDatabaseId(final String databaseName) {
    final State state = getState();
    // if we locked the database before, then use that
    final DatabaseInfo databaseInfo = state.nameToInfo.get(databaseName);
    if (databaseInfo != null) {
      return databaseInfo.id;
    }
    // otherwise check nameToId
    Integer result = state.nameToId.get(databaseName);
    if (result == null) {
      result = databaseService.getDatabaseId(databaseName);
      state.nameToId.put(databaseName, result);
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
    final State state = getState();
    Long result = state.idToIncrementedVersion.get(databaseInfo.id);
    if (result == null) {
      result = databaseService.incrementVersion(databaseInfo);
      state.idToIncrementedVersion.put(databaseInfo.id, result);
    }
    return result;
  }

}
