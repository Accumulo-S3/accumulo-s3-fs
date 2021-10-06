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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.s3.file.S3OutputStreamBase.MINIMUM_BUFFER_SIZE;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// import static org.mockito.ArgumentMatchers.any;
// import static org.mockito.ArgumentMatchers.argThat;
// import static org.mockito.Mockito.doThrow;
// import static org.mockito.Mockito.times;
// import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.easymock.Capture;
import org.easymock.CaptureType;
import org.junit.Before;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.Md5Utils;
import com.google.common.base.Strings;

public class S3NoFlushMultipartOutputStreamTest extends MockS3TestBase {

  private static final String bucket = "bucket";

  public S3NoFlushMultipartOutputStreamTest() {
    super("accS3nf");
  }

  private AmazonS3 mockS3;
  private S3ClientWrapper s3;

  @Before
  public void setup() {
    // mockS3 = spy(new MockAmazonS3Impl());
    s3 = new S3ClientWrapper(mockS3);
    mockS3.createBucket(bucket);
  }

  // @Test
  public void testFileReadOperations() throws IOException {
    String object = "object";

    S3NoFlushMultipartOutputStream stream1 =
        new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
    stream1.write(1);
    stream1.flusherException = new IOException("Io", new SdkClientException(""));
    S3NoFlushMultipartOutputStream stream2 =
        new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
    stream2.write(1);
    stream2.flusherException = new IllegalArgumentException("NotIo");
  }

  // @Test
  public void md5HashesForPutProvided() throws IOException {
    final String object = "object";
    S3NoFlushMultipartOutputStream stream =
        new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
    stream.write(1);
    stream.close();

    final String expectedMd5 = Md5Utils.md5AsBase64(new byte[] {1});
    Capture<PutObjectRequest> capturePutObj = newCapture(CaptureType.ALL);
    expect(mockS3.putObject(capture(capturePutObj))).times(1);
    assertTrue(expectedMd5.equals(capturePutObj.getValue().getMetadata().getContentMD5()));
    // verify(mockS3, times(1))
    // .putObject(argThat(request -> expectedMd5.equals(request.getMetadata().getContentMD5())));
  }

  // @Test
  public void md5HashesForUploadPartProvided() throws IOException {
    final String object = "object";
    S3NoFlushMultipartOutputStream stream =
        new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
    final byte[] content = Strings.repeat("a", MINIMUM_BUFFER_SIZE).getBytes(UTF_8);
    stream.write(content);
    stream.close();

    final String expectedMd5 = Md5Utils.md5AsBase64(content);
    Capture<UploadPartRequest> captureUploadPart = newCapture(CaptureType.ALL);
    expect(mockS3.uploadPart(capture(captureUploadPart))).times(1);
    assertTrue(expectedMd5.equals(captureUploadPart.getValue().getMd5Digest()));
    // verify(mockS3, times(1))
    // .uploadPart(argThat(request -> expectedMd5.equals(request.getMd5Digest())));
  }

  // @Test
  public void errorInFlushDoesNotTriggerUncaughtExceptionHandler() throws InterruptedException {
    AtomicBoolean triggered = new AtomicBoolean(false);
    Thread.UncaughtExceptionHandler old = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> triggered.set(true));

    try {
      String object = "object";
      S3NoFlushMultipartOutputStream stream =
          new S3NoFlushMultipartOutputStream(s3, bucket, object, MINIMUM_BUFFER_SIZE);
      expect(mockS3.uploadPart(anyObject())).andThrow(new AmazonS3Exception("injected"));
      // doThrow(new AmazonS3Exception("injected")).when(mockS3).uploadPart(any());

      Thread.sleep(10); // Yield a little to give the uncaught exception handler a chance to trigger
      assertFalse(triggered.get());
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(old);
    }
  }

  // public static class CausedByMatcher extends BaseMatcher<Exception> {
  //
  // private Class<? extends Throwable> causedby;
  //
  // @Override
  // public boolean matches(Object item) {
  // if (item instanceof Exception) {
  // return ((Exception) item).getCause().getClass().equals(causedby);
  // }
  // return false;
  // }
  //
  // @Override
  // public void describeTo(Description description) {
  //
  // }
  // }
}
