/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual 
 *  contributors. 
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.archive.io.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Authorization;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import org.archive.io.RecordingInputStream;
import org.archive.io.RecordingOutputStream;
import org.archive.io.ReplayInputStream;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;
import org.archive.modules.CrawlURI;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

/**
 * HBase implementation.
 */
public class HBaseWriter extends WriterPoolMember {
  
    private HBaseParameters hbaseOptions;
    private final HTableInterface contentTable;
    private final HTableInterface urlTable;

    private static final Pattern URI_RE_PARSER =
      Pattern.compile("^([^:/?#]+://(?:[^/?#@]+@)?)([^:/?#]+)(.*)$");

    // XXX: Only works with HBASE-7663
    private static final Authorization SECRET = new Authorization("secret");

    private static final MetricRegistry metricRegistry = new MetricRegistry();
    static {
      ConsoleReporter
        .forRegistry(metricRegistry)
        //.outputTo(LogFactory.getLog(HBaseWriter.class))
        .outputTo(System.err)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build()
        .start(1, TimeUnit.MINUTES);
    }

    public HBaseWriter(AtomicInteger serialNo, final WriterPoolSettings settings,
        HBaseParameters parameters) throws IOException {
      super(serialNo, settings, null);
      this.hbaseOptions = parameters;
      Configuration hbaseConfiguration = HBaseConfiguration.create();
      hbaseConfiguration.setStrings(HConstants.ZOOKEEPER_QUORUM,
        hbaseOptions.getZkQuorum().split(","));
      hbaseConfiguration.setInt(hbaseOptions.getZookeeperClientPortKey(),
        hbaseOptions.getZkPort());
      this.contentTable = new MetricsHTable(metricRegistry,
        new HTable(hbaseConfiguration, hbaseOptions.getContentTableName()));
      this.urlTable = new MetricsHTable(metricRegistry,
        new HTable(hbaseConfiguration, hbaseOptions.getUrlTableName()));
    }

    public HTableInterface getContentTable() {
      return contentTable;
    }

    public HTableInterface getUrlTable() {
      return urlTable;
    }

    /**
     * Write the crawled output to the configured HBase table.
     * Write each row key as the url with reverse domain and optionally process any content.
     * 
     * @param curi URI of crawled document
     * @param ip IP of remote machine.
     * @param recordingOutputStream recording input stream that captured the response
     * @param recordingInputStream recording output stream that captured the GET request
     * 
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void write(final CrawlURI curi, final String ip, final RecordingOutputStream recordingOutputStream, 
            final RecordingInputStream recordingInputStream) throws IOException {
        String url = curi.toString();

        boolean isSecret = false;
        Pattern pattern = hbaseOptions.getSecretUriPattern();
        if (pattern != null && pattern.matcher(url).find()) {
          isSecret = true;
        }

        byte[] rowKey = HBaseWriter.createURLKey(url);

        byte[] curiFamily =
            Bytes.toBytes(hbaseOptions.getCuriColumnFamily());
        byte[] contentFamily =
            Bytes.toBytes(hbaseOptions.getContentColumnFamily());

        Put curiPut = new Put(rowKey);

        // status
        curiPut.add(curiFamily,
            Bytes.toBytes(hbaseOptions.getStatusColumnName()),
            Bytes.toBytes(curi.getFetchStatus()));

        // write the target url to the url column
        curiPut.add(curiFamily,
            Bytes.toBytes(hbaseOptions.getUrlColumnName()),
            Bytes.toBytes(url));

        // write the target ip to the ip column
        curiPut.add(curiFamily, 
            Bytes.toBytes(hbaseOptions.getIpColumnName()),
            Bytes.toBytes(ip));

        // path from seed
        String pathFromSeed = curi.getPathFromSeed();
        if (pathFromSeed != null) {
          pathFromSeed = pathFromSeed.trim();
          if (pathFromSeed.length() > 0) {
            curiPut.add(curiFamily,
                Bytes.toBytes(hbaseOptions.getPathFromSeedColumnName()),
                Bytes.toBytes(pathFromSeed));
          }
        }

        // via
        if (curi.getVia() != null) {
          String viaStr = curi.getVia().toString().trim();
          if (viaStr.length() > 0) {
            curiPut.add(curiFamily,
                Bytes.toBytes(hbaseOptions.getViaColumnName()),
                HBaseWriter.createURLKey(viaStr));
          }
        }

        // source tag
        String sourceTag = curi.getSourceTag();
        if (sourceTag != null) {
          curiPut.add(curiFamily,
              Bytes.toBytes(hbaseOptions.getSourceTagColumnName()),
              Bytes.toBytes(sourceTag));
        }

        // content type
        String contentType = curi.getContentType();
        if (contentType != null) {
          // add the mime type of the response 
          curiPut.add(curiFamily,
              Bytes.toBytes(hbaseOptions.getMimeTypeColumnName()),
              Bytes.toBytes(contentType));
        }

        // request
        if (recordingOutputStream.getSize() > 0) {
          ReplayInputStream request = recordingOutputStream.getReplayInputStream();
          try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            request.readContentTo(os);
            curiPut.add(curiFamily,
                Bytes.toBytes(hbaseOptions.getRequestColumnName()),
                os.toByteArray());
          } finally {
            IOUtils.closeStream(request);
          }
        }

        // response

        ReplayInputStream response = recordingInputStream.getReplayInputStream();
        try {

          // headers
          if (response.getHeaderSize() > 0) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            response.readHeaderTo(os);
            curiPut.add(curiFamily,
                Bytes.toBytes(hbaseOptions.getResponseColumnName()),
                os.toByteArray());
          }

          // content
          if (response.getContentSize() > 0) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();            
            response.readContentTo(os);
            byte[] content = os.toByteArray();

            byte[] hashKey = HBaseWriter.createHashKey(content);

            curiPut.add(curiFamily,
                Bytes.toBytes(hbaseOptions.getHashColumnName()), hashKey);

            List<Put> puts = new ArrayList<Put>(2);

            Put put =new Put(hashKey).add(curiFamily, rowKey,
              HConstants.EMPTY_BYTE_ARRAY); // store something useful?
            if (isSecret) {
              // XXX: Only works with HBASE-7663
              put.setAuthorization(SECRET);
            }
            puts.add(put);

            byte[] contentQualifier =
                Bytes.toBytes(hbaseOptions.getContentColumnName());
            // if existence check fails, store an placeholder atomically
            if (contentTable.checkAndPut(hashKey, contentFamily,
                  contentQualifier, null,
                  new Put(hashKey)
                    .add(contentFamily, contentQualifier,
                       HConstants.EMPTY_BYTE_ARRAY))) {
              // and follow up with a (write buffered) store of the real
              // content
              put = new Put(hashKey).add(contentFamily, contentQualifier,
                content);
              if (isSecret) {
                // XXX: Only works with HBASE-7663
                put.setAuthorization(SECRET);
              }
              puts.add(put);
            }

            contentTable.put(puts);
          }
        } finally {
          IOUtils.closeStream(response);
        }

        if (isSecret) {
          // XXX: Only works with HBASE-7663
          curiPut.setAuthorization(SECRET);
        }

        urlTable.put(curiPut);
    }

    @Override
    public void close() throws IOException {
        getContentTable().close();
        getUrlTable().close();
        super.close();
    }

    private static Matcher getMatcher(final String u) {
      if (u == null || u.length() <= 0) {
        return null;
      }
      return URI_RE_PARSER.matcher(u);
    }

    private static String reverseHostname(final String hostname) {
      if (hostname == null) {
        return "";
      }
      StringBuilder sb = new StringBuilder(hostname.length());
      for (StringTokenizer st = new StringTokenizer(hostname, ".", false);
          st.hasMoreElements();) {
        Object next = st.nextElement();
        if (sb.length() > 0) {
          sb.insert(0, ".");
        }
        sb.insert(0, next);
      }
      return sb.toString();
    }

    public static byte[] createURLKey(final String u) {
      Matcher m = getMatcher(u);
      if (m == null || !m.matches()) {
        // dns "URLs" don't match as them
        if (u.startsWith("dns:")) {
          return Bytes.toBytes(reverseHostname(u.substring(4)));
        }
        // If no match, return original String.
        return Bytes.toBytes(u);
      }
      //String scheme = m.group(1);
      String host = m.group(2);
      String path = m.group(3);
      if (path.isEmpty()) {
        path = "/";
      }
      return Bytes.toBytes(reverseHostname(host) + path);
    }

    public static byte[] createHashKey(byte[] content) throws IOException {
      try {
        return MessageDigest.getInstance("SHA1").digest(content);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }      
    }
}