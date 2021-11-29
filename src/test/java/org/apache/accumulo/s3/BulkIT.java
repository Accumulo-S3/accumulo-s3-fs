/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.s3;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import com.amazonaws.services.s3.model.Bucket;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.s3.file.AccumuloNoFlushS3FileSystem;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests Old and New Bulk import
 */
public class BulkIT {
  private static Logger LOG = LoggerFactory.getLogger(BulkIT.class);
  private static final String bucketName = "accumulo";
  public static URL clientPropUrl =
      AccumuloClient.class.getClassLoader().getResource("accumulo-client.properties");
  private static final Path bulkIngestNewBaseDir = new Path("bulkImportTestNew");
  private static final Path bulkIngestOldBaseDir = new Path("bulkImportTestOld");
  private static final FileSystem fs = new AccumuloNoFlushS3FileSystem();
  private AmazonS3 s3;

  private static final int N = 100;
  private static final int COUNT = 2;

  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Before
  public void setupMockS3() throws URISyntaxException, IOException {
    s3 = AmazonS3ClientBuilder.standard()
        .withCredentials(new EnvironmentVariableCredentialsProvider()).build();
    ((AccumuloNoFlushS3FileSystem) fs).initialize(
        new URI(AccumuloNoFlushS3FileSystem.scheme + "://" + bucketName), new Configuration(), s3);

    if (!s3.doesBucketExistV2(bucketName)) {
      CreateBucketRequest req = new CreateBucketRequest(bucketName);
      s3.createBucket(req);
    }
  }

  @After
  public void cleanupMockS3() throws IOException {
    try {
      // s3.deleteBucket(bucketName);
    } catch (AmazonS3Exception e) {
      // some tests clobber this, so ignore if it happens
    }
    FileSystem.closeAll();
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(clientPropUrl.getPath()).build()) {
      System.out
          .println(new Path(AccumuloNoFlushS3FileSystem.scheme + "://" + bucketName).toString());
      runTest(client, "testBulkImportNew", "s3", "new", false);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testOld() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(clientPropUrl.getPath()).build()) {
      System.out
          .println(new Path(AccumuloNoFlushS3FileSystem.scheme + "://" + bucketName).toString());
      runTest(client, "testBulkImportOld", "s3", "old", true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void runTest(AccumuloClient c, String tableName, String filePrefix, String dirSuffix,
      boolean useOld) throws Exception {
    c.tableOperations().create(tableName);
    Path base = new Path("testBulkFail_" + dirSuffix);
    fs.delete(base, true);
    fs.mkdirs(base);
    fs.deleteOnExit(base);
    Path bulkFailures = new Path(base, "failures");
    fs.deleteOnExit(bulkFailures);
    Path files = new Path(base, "files");
    fs.deleteOnExit(files);
    fs.mkdirs(bulkFailures);
    fs.mkdirs(files);

    IngestParams params = new IngestParams(new Properties(), tableName, N);
    params.timestamp = 1;
    params.random = 56;
    params.cols = 1;
    String fileFormat = filePrefix + "rf%02d";
    for (int i = 0; i < COUNT; i++) {
      params.outputFile = new Path(files, String.format(fileFormat, i)).toString();
      params.startRow = N * i;
      TestIngest.ingest(c, fs, params);
    }
    params.outputFile = new Path(files, String.format(fileFormat, N)).toString();
    params.startRow = N;
    params.rows = 1;
    // create an rfile with one entry, there was a bug with this:
    TestIngest.ingest(c, fs, params);

    bulkFailures =
        new Path(AccumuloNoFlushS3FileSystem.scheme + "://" + bucketName + "/", bulkFailures);
    files = new Path(AccumuloNoFlushS3FileSystem.scheme + "://" + bucketName + "/", files);
    bulkLoad(c, tableName, bulkFailures, files, useOld);
    VerifyParams verifyParams = new VerifyParams(new Properties(), tableName, N);
    verifyParams.random = 56;
    for (int i = 0; i < COUNT; i++) {
      verifyParams.startRow = i * N;
      VerifyIngest.verifyIngest(c, verifyParams);
    }
    verifyParams.startRow = N;
    verifyParams.rows = 1;
    VerifyIngest.verifyIngest(c, verifyParams);
    c.tableOperations().delete(tableName);
  }

  @SuppressWarnings("deprecation")
  private static void bulkLoad(AccumuloClient c, String tableName, Path bulkFailures, Path files,
      boolean useOld)
      throws TableNotFoundException, IOException, AccumuloException, AccumuloSecurityException {
    // Make sure the server can modify the files
    if (useOld) {
      LOG.warn(files.toString());
      c.tableOperations().importDirectory(tableName, files.toString(), bulkFailures.toString(),
          false);
    } else {
      // not appending the 'ignoreEmptyDir' method defaults to not ignoring empty directories.
      LOG.warn(files.toString());
      c.tableOperations().importDirectory(files.toString()).to(tableName).load();
      try {
        // if run again, the expected IllegalArgrumentException is thrown
        c.tableOperations().importDirectory(files.toString()).to(tableName).load();
      } catch (IllegalArgumentException ex) {
        // expected exception to be thrown
      }
      // re-run using the ignoreEmptyDir option and no error should be thrown since empty
      // directories will be ignored
      // TODO add ignore empty tests back in version 2.1.0
//      c.tableOperations().importDirectory(files.toString()).to(tableName).ignoreEmptyDir(true)
//          .load();
//      try {
//        // setting ignoreEmptyDir to false, explicitly, results in exception being thrown again.
//        c.tableOperations().importDirectory(files.toString()).to(tableName).ignoreEmptyDir(false)
//            .load();
//      } catch (IllegalArgumentException ex) {
//        // expected exception to be thrown
//      }
    }
  }
}
