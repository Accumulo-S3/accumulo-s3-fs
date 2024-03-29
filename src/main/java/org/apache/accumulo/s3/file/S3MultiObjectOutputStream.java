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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Syncable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class S3MultiObjectOutputStream extends OutputStream implements Syncable {
  private static final Logger LOG = LoggerFactory.getLogger(S3MultiObjectOutputStream.class);
  private static final byte[] EMPTY_BYTES = new byte[0];

  private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
  private final S3ClientWrapper s3;
  private final String bucketName;
  private final String objectPrefix;
  private int partCounter = 0;
  private int flushedPart = -1;
  private boolean closed = false;

  public S3MultiObjectOutputStream(S3ClientWrapper s3, String bucketName, String objectName)
      throws IOException {
    this.s3 = s3;
    this.bucketName = bucketName;
    this.objectPrefix = objectName + "/" + AccumuloMultiObjectS3FileSystem.partPrefix;

    // Upload empty part to ensure existence of the file. If this fails we will throw.
    int initialPart = partCounter++;
    upload(s3, bucketName, objectPrefix, EMPTY_BYTES, initialPart);
    flushedPart = initialPart;
  }

  @Override
  public void write(int b) throws IOException {
    synchronized (outBuffer) {
      if (closed) {
        throw new IOException("write to closed stream");
      }
      outBuffer.write(b);
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    synchronized (outBuffer) {
      if (closed) {
        throw new IOException("write to closed stream");
      }
      outBuffer.write(b);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    synchronized (outBuffer) {
      if (closed) {
        throw new IOException("write to closed stream");
      }
      outBuffer.write(b, off, len);
    }
  }

  @Override
  public void close() throws IOException {
    synchronized (outBuffer) {
      closed = true;
      flushBufferAndWait();
    }
  }

  private static void upload(S3ClientWrapper s3, String bucketName, String objectPrefix,
      byte[] contents, int partNumber) throws IOException {
    ByteArrayInputStream objectContents = new ByteArrayInputStream(contents);
    ObjectMetadata meta = new ObjectMetadata();
    meta.setContentLength(contents.length);
    String key = objectPrefix + partNumber;
    PutObjectRequest req = new PutObjectRequest(bucketName, key, objectContents, meta);
    s3.putObject(req);
  }

  private void flushBufferAndWait() throws IOException {
    int partNumber = -1;
    byte[] contents;
    synchronized (outBuffer) {
      // write a part if there's something to write
      if (outBuffer.size() > 0) {
        contents = outBuffer.toByteArray();
        outBuffer.reset();
        partNumber = partCounter++;
      } else {
        // nothing to flush, but we need to make sure previous flushes have completed
        int flushTarget = partCounter - 1;
        while (flushedPart < flushTarget) {
          try {
            outBuffer.wait();
          } catch (InterruptedException e) {
            // don't allow interruption of flush since this could be the WAL
          }
        }
        return;
      }
    }
    // flush the buffer outside of the monitor so that we can support multiple concurrent flushes
    boolean success = false;
    while (!success) {
      try {
        upload(s3, bucketName, objectPrefix, contents, partNumber);
        success = true;
      } catch (IOException e) {
        LOG.warn("Exception when flushing to S3, retrying", e);
      }
    }
    // wait until all previous flushes have completed, then signal completion of this flush
    synchronized (outBuffer) {
      while (flushedPart < partNumber - 1) {
        try {
          outBuffer.wait();
        } catch (InterruptedException e) {
          // don't allow interruption of flush since this could be the WAL
        }
      }
      flushedPart++;
      outBuffer.notifyAll();
    }
  }

  @Override
  public void flush() throws IOException {
    flushBufferAndWait();
  }

  // @Override
  public void sync() throws IOException {
    flush();
  }

  @Override
  public void hflush() throws IOException {
    flush();
  }

  @Override
  public void hsync() throws IOException {
    flush();
  }
}
