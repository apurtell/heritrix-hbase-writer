package org.archive.io.hbase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;

public class HBaseWriterPool extends WriterPool {

  private HBaseParameters parameters;

  public HBaseWriterPool(AtomicInteger serial, WriterPoolSettings settings,
      int poolMaximumActive, int poolMaximumWait, HBaseParameters parameters) {
    super(serial, settings, poolMaximumActive, poolMaximumWait);
    this.parameters = parameters;
  }

  @Override
  protected WriterPoolMember makeWriter() {
    try {
      return new HBaseWriter(getSerialNo(), getSettings(), parameters);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't create a " +
        HBaseWriter.class.getName() + " writer object", e);
    }
  }

}