package org.seqdoop.hadoop_bam.spark;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

public class Merger {

  private final FileSystemWrapper fileSystemWrapper;

  public Merger() {
    fileSystemWrapper = new HadoopFileSystemWrapper();
  }

  public void mergeParts(Configuration conf, String partDirectory, String outputFile) throws IOException {
    fileSystemWrapper.concat(conf, fileSystemWrapper.listDirectory(conf, partDirectory), outputFile);
  }

}
