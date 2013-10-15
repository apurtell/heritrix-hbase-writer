package org.archive.io.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * A statshtable-inspired HTable wrapper that collects per operation and per
 * server metrics.
 */
public class MetricsHTable implements HTableInterface {

  private final MetricRegistry metricRegistry;
  private final ConcurrentSkipListMap<String, Timer> timers =
    new ConcurrentSkipListMap<String, Timer>();
  private final HTable hTable;
  private final String tableName;

  /**
   * @param hTable The HTable to wrap
   * @param scope The metrics scope, for example a job ID
   */
  public MetricsHTable(MetricRegistry metricRegistry, HTable hTable) {
    this.metricRegistry = metricRegistry;
    this.hTable = hTable;
    this.tableName = Bytes.toStringBinary(hTable.getTableName());
  }

  @Override
  public TableName getName() {
    return hTable.getName();
  }

  @Override
  public byte[] getTableName() {
    return hTable.getTableName();
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @Override
  public Configuration getConfiguration() {
    return hTable.getConfiguration();
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return timedTableOp("getTableDescriptor", (byte[])null, new Callable<HTableDescriptor>() {
      public HTableDescriptor call() throws IOException {
        return hTable.getTableDescriptor();
      }
    });
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    return timedTableOp("exists", get.getRow(), new Callable<Boolean>() {
      public Boolean call() throws IOException {
        return hTable.exists(get);
      }
    });
  }

  @Override
  public void batch(final List<? extends Row> actions, final Object[] results)
      throws IOException, InterruptedException {
    List<byte[]> keys = Lists.newArrayList();
    for (Row action: actions) {
      keys.add(action.getRow());
    }
    timedTableOp("batch", keys, new Callable<Void>() {
      public Void call() throws IOException, InterruptedException {
        hTable.batch(actions, results);
        return null;
      }
    });
  }

  @Override
  public Object[] batch(final List<? extends Row> actions)
      throws IOException, InterruptedException {
    List<byte[]> keys = Lists.newArrayList();
    for (Row action: actions) {
      keys.add(action.getRow());
    }
    return timedTableOp("batch", keys, new Callable<Object[]>() {
      public Object[] call() throws IOException, InterruptedException {
        return hTable.batch(actions);
      }
    });
  }

  @Override
  public <R> void batchCallback(final List<? extends Row> actions,
      final Object[] results, final Callback<R> callback)
      throws IOException, InterruptedException {
    List<byte[]> keys = Lists.newArrayList();
    for (Row action: actions) {
      keys.add(action.getRow());
    }
    timedTableOp("batchCallback", keys, new Callable<Void>() {
      public Void call() throws IOException, InterruptedException {
        hTable.batchCallback(actions, results, callback);
        return null;
      }
    });
  }

  @Override
  public <R> Object[] batchCallback(final List<? extends Row> actions,
      final Callback<R> callback) throws IOException, InterruptedException {
    List<byte[]> keys = Lists.newArrayList();
    for (Row action: actions) {
      keys.add(action.getRow());
    }
    return timedTableOp("batchCallback", keys, new Callable<Object[]>() {
      public Object[] call() throws IOException, InterruptedException {
        return hTable.batchCallback(actions, callback);
      }
    });
  }

  @Override
  public Boolean[] exists(final List<Get> gets) throws IOException {
    List<byte[]> keys = Lists.newArrayList();
    for (Get get: gets) {
      keys.add(get.getRow());
    }
    return timedTableOp("exists", keys, new Callable<Boolean[]>() {
      public Boolean[] call() throws IOException {
        return hTable.exists(gets);
      }
    });
  }

  @Override
  public Result get(final Get get) throws IOException {
    return timedTableOp("get", get.getRow(), new Callable<Result>() {
      public Result call() throws IOException {
        return hTable.get(get);
      }
    });
  }

  @Override
  public Result[] get(final List<Get> gets) throws IOException {
    List<byte[]> keys = Lists.newArrayList();
    for (Get get: gets) {
      keys.add(get.getRow());
    }
    return timedTableOp("get", keys, new Callable<Result[]>() {
      public Result[] call() throws IOException {
        return hTable.get(gets);
      }
    });
  }

  @Override
  public Result getRowOrBefore(final byte[] row, final byte[] family) throws IOException {
    return timedTableOp("getRowOrBefore", row, new Callable<Result>() {
      public Result call() throws IOException {
        return hTable.getRowOrBefore(row, family);
      }
    });
  }

  @Override
  public ResultScanner getScanner(final Scan scan) throws IOException {
    return timedTableOp("getScanner", scan.getStartRow(), new Callable<ResultScanner>() {
      public ResultScanner call() throws IOException {
        return new WrappedResultScanner(hTable.getScanner(scan));
      }
    });
  }

  @Override
  public ResultScanner getScanner(final byte[] family) throws IOException {
    return timedTableOp("getScanner", (byte[])null, new Callable<ResultScanner>() {
      public ResultScanner call() throws IOException {
        return new WrappedResultScanner(hTable.getScanner(family));
      }
    });
  }

  @Override
  public ResultScanner getScanner(final byte[] family, final byte[] qualifier) throws IOException {
    return timedTableOp("getScanner", (byte[])null, new Callable<ResultScanner>() {
      public ResultScanner call() throws IOException {
        return new WrappedResultScanner(hTable.getScanner(family, qualifier));
      }
    });
  }

  private class WrappedResultIterator implements Iterator<Result> {
    private final Iterator<Result> iterator;

    public WrappedResultIterator(Iterator<Result> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      try {
        return timedScannerOp("hasNext", new Callable<Boolean>() {
          public Boolean call() {
            return iterator.hasNext();
          }
        });
      } catch (IOException e) {
        // Shouldn't happen but we need to let this bubble up if so
        throw new RuntimeException(e);
      }
    }

    @Override
    public Result next() {
      try {
        return timedScannerOp("next", new Callable<Result>() {
          public Result call() {
            return iterator.next();
          }
        });
      } catch (IOException e) {
        // Shouldn't happen but we need to let this bubble up if so
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      try {
        timedScannerOp("remove", new Callable<Void>() {
          public Void call() {
            iterator.remove();
            return null;
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class WrappedResultScanner implements ResultScanner {
    private final ResultScanner scanner;

    public WrappedResultScanner(ResultScanner scanner) {
      this.scanner = scanner;
    }

    @Override
    public Iterator<Result> iterator() {
      return new WrappedResultIterator(scanner.iterator());
    }

    @Override
    public Result next() throws IOException {
      return timedScannerOp("next", new Callable<Result>() {
        public Result call() throws IOException {
          return scanner.next();
        }
      });
    }

    @Override
    public Result[] next(final int nbRows) throws IOException {
      return timedScannerOp("next[]", new Callable<Result[]>() {
        public Result[] call() throws IOException {
          return scanner.next(nbRows);
        }
      });
    }

    @Override
    public void close() {
      try {
        timedScannerOp("close", new Callable<Void>() {
          public Void call() {
            scanner.close();
            return null;
          }
        });
      } catch (IOException e) {
        // Shouldn't happen but we need to let this bubble up if so
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void put(final Put put) throws IOException {
    timedTableOp("put", put.getRow(), new Callable<Void>() {
      public Void call() throws IOException {
        hTable.put(put);
        return null;
      }
    });
  }

  @Override
  public void put(final List<Put> puts) throws IOException {
    List<byte[]> keys = Lists.newArrayList();
    for (Put put: puts) {
      keys.add(put.getRow());
    }
    timedTableOp("put", keys,  new Callable<Void>() {
      public Void call() throws IOException {
        hTable.put(puts);
        return null;
      }
    });
  }

  @Override
  public boolean checkAndPut(final byte[] row, final byte[] family, final byte[] qualifier,
      final byte[] value, final Put put) throws IOException {
    return timedTableOp("checkAndPut", row, new Callable<Boolean>() {
      public Boolean call() throws IOException {
        return hTable.checkAndPut(row, family, qualifier, value, put);
      }
    });
  }

  @Override
  public void delete(final Delete delete) throws IOException {
    timedTableOp("delete", delete.getRow(), new Callable<Void>() {
      public Void call() throws IOException {
        hTable.delete(delete);
        return null;
      }
    });
  }

  @Override
  public void delete(final List<Delete> deletes) throws IOException {
    List<byte[]> keys = Lists.newArrayList();
    for (Delete delete: deletes) {
      keys.add(delete.getRow());
    }
    timedTableOp("delete", keys, new Callable<Void>() {
      public Void call() throws IOException {
        hTable.delete(deletes);
        return null;
      }
    });
  }

  @Override
  public boolean checkAndDelete(final byte[] row, final byte[] family, final byte[] qualifier,
      final byte[] value, final Delete delete) throws IOException {
    return timedTableOp("checkAndDelete", row, new Callable<Boolean>() {
      public Boolean call() throws IOException {
        return hTable.checkAndDelete(row, family, qualifier, value, delete);
      }
    });
  }

  @Override
  public void mutateRow(final RowMutations rm) throws IOException {
    timedTableOp("mutateRow", rm.getRow(), new Callable<Void>() {
      public Void call() throws IOException {
        hTable.mutateRow(rm);
        return null;
      }
    });
  }

  @Override
  public Result append(final Append append) throws IOException {
    return timedTableOp("append", append.getRow(), new Callable<Result>() {
      public Result call() throws IOException {
        return hTable.append(append);
      }
    });
  }

  @Override
  public Result increment(final Increment increment) throws IOException {
    return timedTableOp("increment", increment.getRow(), new Callable<Result>() {
      public Result call() throws IOException {
        return hTable.increment(increment);
      }
    });
  }

  @Override
  public long incrementColumnValue(final byte[] row, final byte[] family,
      final byte[] qualifier, final long amount) throws IOException {
    return incrementColumnValue(row, family, qualifier, amount, true);
  }

  @Override
  public long incrementColumnValue(final byte[] row, final byte[] family,
      final byte[] qualifier, final long amount, final boolean writeToWAL)
      throws IOException {
    return timedTableOp("incrementColumnValue", row, new Callable<Long>() {
      @SuppressWarnings("deprecation")
      public Long call() throws IOException {
        return hTable.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
      }
    });
  }

  @Override
  public long incrementColumnValue(final byte[] row, final byte[] family,
      final byte[] qualifier, final long amount, final Durability durability)
      throws IOException {
    return timedTableOp("incrementColumnValue", row, new Callable<Long>() {
      public Long call() throws IOException {
        return hTable.incrementColumnValue(row, family, qualifier, amount, durability);
      }
    });
  }

  @Override
  public boolean isAutoFlush() {
    return hTable.isAutoFlush();
  }

  @Override
  public void flushCommits() throws IOException {
    timedTableOp("flushCommits", (byte[])null, new Callable<Void>() {
      public Void call() throws IOException {
        hTable.flushCommits();
        return null;
      }
    });
  }

  @Override
  public void close() throws IOException {
    hTable.close();
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setAutoFlush(boolean autoFlush) {
    hTable.setAutoFlush(autoFlush);
  }

  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    hTable.setAutoFlush(autoFlush, clearBufferOnFail);
  }

  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    hTable.setAutoFlushTo(autoFlush);
  }

  @Override
  public long getWriteBufferSize() {
    return hTable.getWriteBufferSize();
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    hTable.setWriteBufferSize(writeBufferSize);
  }

  //-----------------------------------------------------------------------

  // Coprocessor methods. HTableInterface must implement them but it is not
  // clear if we will need to support coprocessor use/test cases.

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    return hTable.coprocessorService(row);
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey,
    byte[] endKey, Call<T, R> callable) throws ServiceException, Throwable {
    return hTable.coprocessorService(service, startKey, endKey, callable);
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey,
      byte[] endKey, Call<T, R> callable, Callback<R> callback)
      throws Throwable {
    hTable.coprocessorService(service, startKey, endKey, callable, callback);
  }

  //-----------------------------------------------------------------------

  protected <T> T timedTableOp(String opName, byte[] key, Callable<T> callable) throws IOException {
    List<byte[]> keys = Lists.newArrayList();
    if (key != null) {
      keys.add(key);
    }
    return timedTableOp(opName, keys, callable);
  }

  protected <T> T timedTableOp(String opName, List<byte[]> keys, Callable<T> callable) throws IOException {
    try {
      long before = System.nanoTime();
      T result = callable.call();
      update(opName, keys != null ? keys : new ArrayList<byte[]>(), System.nanoTime() - before);
      return result;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      // Wrap all checked exceptions into an IOE, they are most likely an IOE anyway
      if (e instanceof IOException) {
        throw (IOException)e;
      } else {
        throw new IOException(e);
      }
    }
  }

  // If an iterator call takes less than this duration, assume it is processing cached batch
  // results and ignore the data point. This is a great idea borrowed from statshtable.
  private static final long ITERATOR_THRESHOLD = TimeUnit.MICROSECONDS.toNanos(10);

  protected <T> T timedScannerOp(String opName, Callable<T> callable) throws IOException {
    // With a scanner op, we do not know the row keys involved until the op returns
    // and has a return type of Result or Result[]
    try {
      long before = System.nanoTime();
      T result = callable.call();
      long duration = System.nanoTime() - before;
      if (duration > ITERATOR_THRESHOLD) {
        List<byte[]> keys = Lists.newArrayList();
        if (result != null) {
          if (result instanceof Result) {
            keys.add(((Result)result).getRow());
          } else if (result instanceof Result[]) {
            for (Result r: (Result[])result) {
              keys.add(r.getRow());
            }
          }
        }
        update(opName, keys, duration);
      }
      return result;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      // Wrap all checked exceptions into an IOE, they are most likely an IOE anyway
      if (e instanceof IOException) {
        throw (IOException)e;
      } else {
        throw new IOException(e);
      }
    }
  }

  protected Timer getOrAddTimer(String... name) {
    String metricName = MetricRegistry.name(getClass(), name);
    Timer t = timers.get(metricName);
    if (t == null) {
      t = metricRegistry.timer(metricName);
      timers.putIfAbsent(metricName, t);
    }
    return t;
  }

  protected void update(String opName, List<byte[]> keys, long duration) throws IOException {
    // Map keys to regions and servers
    Set<String> regionNames = Sets.newHashSet();
    Set<String> serverNames = Sets.newHashSet();
    if (keys != null) {
      for (byte[] key : keys) {
        // Use cached information where possible
        HRegionLocation regionLocation = hTable.getRegionLocation(key, false);
        regionNames.add(regionLocation.getRegionInfo().getEncodedName());
        serverNames.add(regionLocation.getHostname());
      }
    }

    // Latencies by op
    getOrAddTimer(opName).update(duration, TimeUnit.NANOSECONDS);

    // Latencies by region
    for (String regionName : regionNames) {
      getOrAddTimer(opName, tableName + "|" + regionName).update(duration, TimeUnit.NANOSECONDS);
    }

    // Latencies by server
    for (String serverName : serverNames) {
      getOrAddTimer(opName, serverName).update(duration, TimeUnit.NANOSECONDS);
    }
  }

}
