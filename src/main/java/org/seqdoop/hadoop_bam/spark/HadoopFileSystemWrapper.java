package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.seqdoop.hadoop_bam.spark.htsjdk_contrib.SeekableBufferedStream;
import org.seqdoop.hadoop_bam.util.WrapSeekable;

class HadoopFileSystemWrapper implements FileSystemWrapper {

  @Override
  public SeekableStream open(Configuration conf, String path) throws IOException {
    return new SeekableBufferedStream(WrapSeekable.openPath(conf, new Path(path)));
  }

  @Override
  public OutputStream create(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.create(p);
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

  @Override
  public boolean isDirectory(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return fileSystem.isDirectory(p);
  }

  @Override
  public List<String> listDirectory(Configuration conf, String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem(conf);
    return Arrays.stream(fileSystem.listStatus(p))
        .map(fs -> fs.getPath().toUri().toString())
        .sorted()
        .collect(Collectors.toList());
  }

  @Override
  public void concat(Configuration conf, List<String> parts, String path) throws IOException {
    Path p = new Path(path); // TODO: check same fs as parts
    FileSystem fileSystem = p.getFileSystem(conf);
    fileSystem.create(p).close(); // target must already exist for concat
    try {
      concat(parts, p, fileSystem);
    } catch (UnsupportedOperationException e) {
      System.out.println("Concat not supported, merging serially");
      try (OutputStream out = create(conf, path)) {
        for (String part : parts) {
          try (InputStream in = open(conf, part)) {
            IOUtils.copyBytes(in, out, conf, false);
          }
          fileSystem.delete(new Path(part), false);
        }
      }
    }
  }

  static void concat(List<String> parts, Path outputPath, FileSystem filesystem) throws IOException {
    org.apache.hadoop.fs.Path[] fsParts = parts.stream()
        .map(p -> new org.apache.hadoop.fs.Path(p))
        .collect(Collectors.toList())
        .toArray(new org.apache.hadoop.fs.Path[parts.size()]);
    filesystem.concat(new org.apache.hadoop.fs.Path(outputPath.toUri()), fsParts);
  }

}
