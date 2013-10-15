package org.archive.modules.writer;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestHBaseWriterProcessor {

  /** The hwproc. */
  HBaseWriterProcessor hwproc;

  /**
   * Creates the h base writer processor.
   */
  @BeforeClass()
  public void createHBaseWriterProcessor() {
    hwproc = new HBaseWriterProcessor();
  }

  /**
   * Test h base writer processor integrity.
   */
  @Test()
  public void testHBaseWriterProcessorIntegrity() {
    Assert.assertNotNull(hwproc);
    Assert.assertEquals(hwproc.getURICount(), 0);
  }

}
