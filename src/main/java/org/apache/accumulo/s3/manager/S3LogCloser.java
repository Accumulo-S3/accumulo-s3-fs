package org.apache.accumulo.s3.manager;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.recovery.LogCloser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class S3LogCloser implements LogCloser {
  @Override
  public long close(AccumuloConfiguration accumuloConfiguration, Configuration configuration, VolumeManager volumeManager, Path path) throws IOException {
    return 0;
  }
}
