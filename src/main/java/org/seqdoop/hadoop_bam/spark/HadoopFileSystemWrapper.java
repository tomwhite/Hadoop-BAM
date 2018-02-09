package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

class HadoopFileSystemWrapper implements FileSystemWrapper {

  @Override
  public SeekableStream open(Configuration conf, String path) throws IOException {
    return new SeekableBufferedStream(WrapSeekable.openPath(conf, new Path(path)));
  }

  @Override
  public boolean exists(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.exists(p);
  }

  @Override
  public long getFileLength(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.getFileStatus(p).getLen();
  }
}
