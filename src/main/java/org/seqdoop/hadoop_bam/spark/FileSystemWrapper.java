package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;

/**
 * A wrapper around Hadoop and NIO filesystems so users can choose a single one to use for all
 * filesystem operations.
 */
interface FileSystemWrapper extends Serializable {

  SeekableStream open(Configuration conf, String path) throws IOException;

  boolean exists(Configuration conf, String path) throws IOException;

  long getFileLength(Configuration conf, String path) throws IOException;

}
