package de.dbathon.jds.service;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class DatabaseServiceTest {

  @Test
  public void testIncrementVersionString() {
    String currentVersion = DatabaseService.INITIAL_VERSION;
    for (int i = 0; i < 200000; ++i) {
      final String nextVersion = DatabaseService.incrementVersionString(currentVersion);
      assertNotEquals(currentVersion, nextVersion);
      assertTrue(currentVersion.compareTo(nextVersion) < 0);
      currentVersion = nextVersion;
    }
  }

}
