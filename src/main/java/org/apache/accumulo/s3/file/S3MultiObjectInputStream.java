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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class S3MultiObjectInputStream extends InputStream implements Seekable, PositionedReadable {
  public static final Logger log = LoggerFactory.getLogger(S3MultiObjectInputStream.class);
  private final S3ClientWrapper s3;
  private final String bucketName;
  private final String objectPrefix;
  private long pos = 0;
  private long bufferStart = 0;
  private int objectId = 0;
  private byte[] buffer;
  private boolean endReached = false;

  public S3MultiObjectInputStream(S3ClientWrapper s3, String bucketName, String objectName)
      throws IOException {
    this.s3 = s3;
    this.bucketName = bucketName;
    this.objectPrefix = objectName + "/" + AccumuloMultiObjectS3FileSystem.partPrefix;
    fillBuffer();
    if (buffer == null) {
      throw new FileNotFoundException();
    }
  }

  private synchronized void fillBuffer() throws IOException {
    if (endReached) {
      return;
    }
    if (buffer != null) {
      bufferStart += buffer.length;
    }
    String nextObjectName = objectPrefix + objectId++;
    GetObjectRequest req = new GetObjectRequest(bucketName, nextObjectName);
    S3Object object = s3.getObject(req);
    if (object == null) {
      buffer = null;
      endReached = true;
      return;
    }
    long length = object.getObjectMetadata().getContentLength();
    buffer = new byte[(int) length];
    try (S3ObjectInputStream content = object.getObjectContent()) {
      int offset = 0;
      while (offset < buffer.length) {
        int read = content.read(buffer, offset, buffer.length - offset);
        if (read <= 0) {
          throw new IOException("read terminated before end of object");
        }
        offset += read;
      }
    } finally {
      object.close();
    }
  }

  @Override
  public synchronized int read() throws IOException {
    if (endReached) {
      return -1;
    }
    while (buffer == null || pos >= bufferStart + buffer.length) {
      fillBuffer();
      if (endReached) {
        return -1;
      }
    }
    return buffer[(int) (pos++ - bufferStart)] & 0xFF;
  }

  @Override
  public synchronized int read(final byte[] b, final int offset, final int length)
      throws IOException {
    int off = offset;
    int len = length;
    int totalRead = 0;
    while (len > 0) {
      int bufferPos = (int) (pos - bufferStart);
      if (buffer == null || bufferPos >= buffer.length) {
        fillBuffer();
        if (endReached) {
          return totalRead > 0 ? totalRead : -1;
        }
        continue;
      }
      int avilableToRead = buffer.length - bufferPos;
      int read = Math.min(len, avilableToRead);
      System.arraycopy(buffer, bufferPos, b, off, read);
      len -= read;
      off += read;
      pos += read;
      totalRead += read;
    }
    return totalRead;
  }

  public void seek(long pos) throws IOException {
    if (pos < 0) {
      log.error("Trying to seek to a negative number " + pos);
      return;
    }
    if (this.pos > pos) {
      reset();
    }
    while (pos > this.pos) {
      if (read() == -1) {
        throw new IOException("Cannot seek beyond the length of the file.");
      }
    }
  }

  @Override
  public synchronized void reset() throws IOException {
    pos = 0;
    bufferStart = 0;
    objectId = 0;
    endReached = false;
    buffer = null;
    fillBuffer();
  }

  public long getPos() throws IOException {
    return pos;
  }

  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new UnsupportedOperationException();
  }

  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void readFully(long position, byte[] buffer) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {}

}
