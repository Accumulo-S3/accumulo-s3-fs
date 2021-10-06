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

public class S3NoFlushMultipartOutputStream extends S3OutputStreamBase {

  public S3NoFlushMultipartOutputStream(S3ClientWrapper s3, String bucketName, String objectName,
      int bufferSize) {
    super(s3, bucketName, objectName, bufferSize);
  }

  @Override
  protected byte[] copyToFlushBuffer(byte[] circularBuffer, long startPos, long endPos) {
    byte[] flushBuffer = new byte[(int) (endPos - startPos)];
    int circularStart = (int) (startPos % circularBuffer.length);
    int circularEnd = (int) (endPos % circularBuffer.length);
    if (circularStart < circularEnd || circularEnd == 0) {
      System.arraycopy(circularBuffer, circularStart, flushBuffer, 0, flushBuffer.length);
    } else {
      int firstCopyLength = circularBuffer.length - circularStart;
      System.arraycopy(circularBuffer, circularStart, flushBuffer, 0, firstCopyLength);
      System.arraycopy(circularBuffer, 0, flushBuffer, firstCopyLength,
          flushBuffer.length - firstCopyLength);
    }
    return flushBuffer;
  }

  @Override
  public void flush() {
    // do nothing
  }
}
