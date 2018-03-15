package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.seekablestream.SeekablePathStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.seqdoop.hadoop_bam.util.NIOFileUtil;

class NioFileSystemWrapper implements FileSystemWrapper {

  @Override
  public SeekableStream open(Configuration conf, String path) throws IOException {
    return new SeekableBufferedStream(new SeekablePathStream(NIOFileUtil.asPath(path)));
  }

  @Override
  public OutputStream create(Configuration conf, String path) throws IOException {
    return Files.newOutputStream(NIOFileUtil.asPath(path));
  }

  @Override
  public boolean exists(Configuration conf, String path) {
    return Files.isRegularFile(NIOFileUtil.asPath(path));
  }

  @Override
  public long getFileLength(Configuration conf, String path) throws IOException {
    return Files.size(NIOFileUtil.asPath(path));
  }

  @Override
  public List<String> listDirectory(Configuration conf, String path) throws IOException {
    return null;
  }

  @Override
  public void concat(Configuration conf, List<String> parts, String path) throws IOException {
    try (OutputStream out = create(conf, path)) {
      for (final String part : parts) {
        Files.copy(NIOFileUtil.asPath(part), out);
      }
    }
  }
}
