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
package org.apache.accumulo.s3.file;

import static org.apache.accumulo.s3.file.AccumuloS3FileSystemBase.USE_STATIC_CLIENT;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;

public class MockS3TestBase {

  protected final String bucketName;
  protected final String scheme;

  protected AmazonS3 s3;

  public MockS3TestBase(String scheme) {
    bucketName = "accumulo.s3";
    this.scheme = scheme;
  }

  protected FileSystem getFileSystem() {
    return getFileSystemForScheme(scheme);
  }

  protected FileSystem getFileSystemForScheme(String scheme) {
    try {
      final URI uri = new URI(scheme + "://" + bucketName);
      final Configuration conf = getConf();

      String key = conf.get("fs.s3a.access.key");
      String secret = conf.get("fs.s3a.secret.key");

      AccumuloS3FileSystemBase.staticClient = s3;
      return FileSystem.get(uri, conf);
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setupMockS3() {
    System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
    BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials("KEY", "SECRET");
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    clientConfiguration.setProtocol(Protocol.HTTPS);
    s3 = AmazonS3Client.builder()
        .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
        .disableChunkedEncoding().withClientConfiguration(clientConfiguration).build();

    // CreateBucketRequest req = new CreateBucketRequest(bucketName);
    // s3.createBucket(req);
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

  protected Configuration getConf() {
    final Configuration configuration = new Configuration();

    for (Map.Entry<String,String> stringStringEntry : configuration) {
      System.out.println(stringStringEntry.getKey() + " : " + stringStringEntry.getValue());
    }
    // Explicitly check for S3 environment variables
    String keyId = System.getenv("AWS_ACCESS_KEY_ID");
    String accessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (keyId != null && accessKey != null) {
      configuration.set("fs.s3.awsAccessKeyId", keyId);
      configuration.set("fs.s3n.awsAccessKeyId", keyId);
      configuration.set("fs.s3a.access.key", keyId);
      configuration.set("fs.s3.awsSecretAccessKey", accessKey);
      configuration.set("fs.s3n.awsSecretAccessKey", accessKey);
      configuration.set("fs.s3a.secret.key", accessKey);
      // configuration.set("fs.s3a.endpoint", "");
      // configuration.set("test.fs.s3a.encryption.enabled", "false");

      String sessionToken = System.getenv("AWS_SESSION_TOKEN");
      if (sessionToken != null) {
        configuration.set("fs.s3a.session.token", sessionToken);
      }
    }
    configuration.setBoolean(USE_STATIC_CLIENT, true);

    return configuration;
  }
}
