package org.archive.io.hbase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.archive.io.hbase.HBaseWriter;
import org.archive.io.hbase.HBaseParameters;

public class TestHBaseWriter {

  /** The zkQuorum. */
  String zkQuorum = "localhost";

  /** The zkClientPort. */
  int zkClientPort = 2181;

  /** The table. */
  String table = "test";

  /** The pool maximum active. */
  int poolMaximumActive = 10;

  /** The pool maximum wait. */
  int poolMaximumWait = 20;

  /** The hw. */
  HBaseWriter hw;

  @Test
  public void testCreateHBaseWriter() throws IOException {
    // Test
    try {
      hw = new HBaseWriter(new AtomicInteger(), null, null);
      Assert.assertNull(hw);
    } catch (IllegalArgumentException e) {
      Assert.assertNotNull(e);
    }

    try {
      hw = new HBaseWriter(new AtomicInteger(), null, new HBaseParameters());
      Assert.assertNull(hw);
    } catch (IllegalStateException e) {
      Assert.assertNotNull(e);
    }
  }
}
