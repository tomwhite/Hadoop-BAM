package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.CRAMCRAIIndexer;
import htsjdk.samtools.CRAMFileReader;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMFileSpan;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReader.PrimitiveSamReaderToSamReaderAdapter;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.CRAIEntry;
import htsjdk.samtools.cram.CRAIIndex;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.seqdoop.hadoop_bam.util.NIOFileUtil;
import scala.Tuple2;

//TODO: remove duplication with BamSource
class CramSource implements Serializable {

  private final FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  public SAMFileHeader getFileHeader(JavaSparkContext jsc, String path, ValidationStringency stringency,
      String referenceSourcePath) throws IOException {
    // TODO: support header merging
    Configuration conf = jsc.hadoopConfiguration();
    String firstBamPath;
    if (fileSystemWrapper.isDirectory(conf, path)) {
      Optional<String> firstPath = fileSystemWrapper.listDirectory(conf, path).stream()
          .filter(f -> !(f.startsWith(".") || f.startsWith("_")))
          .findFirst();
      if (!firstPath.isPresent()) {
        throw new IllegalArgumentException("No files found in " + path);
      }
      firstBamPath = firstPath.get();
    } else {
      firstBamPath = path;
    }
    try (SamReader samReader = createSamReader(conf, firstBamPath, stringency, referenceSourcePath)) {
      return samReader.getFileHeader();
    }
  }

  public JavaRDD<SAMRecord> getReads(JavaSparkContext jsc, String path, int splitSize,
      ValidationStringency validationStringency, String referenceSourcePath) throws IOException {
    // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat

    final Configuration conf = jsc.hadoopConfiguration();
    if (splitSize > 0) {
      conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
    }

    long cramFileLength = fileSystemWrapper.getFileLength(conf, path);
    List<Long> containerOffsets = getContainerOffsetsFromIndex(conf, path, cramFileLength);
    Broadcast<List<Long>> containerOffsetsBroadcast = jsc.broadcast(containerOffsets);

    SerializableHadoopConfiguration confSer = new SerializableHadoopConfiguration(conf);
    return jsc
        .newAPIHadoopFile(path, FileSplitInputFormat.class, Void.class, FileSplit.class, conf)
        .flatMap((FlatMapFunction<Tuple2<Void, FileSplit>, SAMRecord>) t2 -> {
          FileSplit fileSplit = t2._2();
          List<Long> offsets = containerOffsetsBroadcast.getValue();
          long newStart = nextContainerOffset(offsets, fileSplit.getStart());
          long newEnd = nextContainerOffset(offsets, fileSplit.getStart() + fileSplit.getLength());
          Configuration c = confSer.getConf();
          String p = fileSplit.getPath().toString();
          CRAMFileReader cramFileReader = createCramFileReader(c, p, validationStringency, referenceSourcePath);
          // TODO: test edge cases
          // Subtract one from end since CRAMIterator's boundaries are inclusive
          SAMFileSpan splitSpan = new BAMFileSpan(new Chunk(newStart << 16, (newEnd - 1) << 16));
          return cramFileReader.getIterator(splitSpan);
        });
  }

  private List<Long> getContainerOffsetsFromIndex(Configuration conf, String path, long cramFileLength)
      throws IOException {
    // TODO: formalize finding the index file - perhaps use createSamReader call below
    try (SeekableStream in = fileSystemWrapper.open(conf, path + ".crai")) {
      List<Long> containerOffsets = new ArrayList<Long>();
      CRAIIndex index = CRAMCRAIIndexer.readIndex(in);
      for (CRAIEntry entry : index.getCRAIEntries()) {
        containerOffsets.add(entry.containerStartOffset);
      }
      containerOffsets.add(cramFileLength);
      return containerOffsets;
    }
  }

  private static long nextContainerOffset(List<Long> containerOffsets, long position) {
    int index = Collections.binarySearch(containerOffsets, position);
    long offset;
    if (index >= 0) {
      offset = containerOffsets.get(index);
    } else {
      int insertionPoint = -index - 1;
      if (insertionPoint == containerOffsets.size()) {
        throw new IllegalStateException("Could not find position " + position + " in " +
            "container offsets: " + containerOffsets);
      }
      offset = containerOffsets.get(insertionPoint);
    }
    return offset;
  }

  private CRAMFileReader createCramFileReader(Configuration conf, String path, ValidationStringency stringency, String referenceSourcePath) throws IOException {
    return (CRAMFileReader) ((PrimitiveSamReaderToSamReaderAdapter) createSamReader(conf, path, stringency, referenceSourcePath)).underlyingReader();
  }

  private SamReader createSamReader(Configuration conf, String path, ValidationStringency stringency, String referenceSourcePath) throws IOException {
    SeekableStream in = fileSystemWrapper.open(conf, path);
    //SeekableStream indexStream = findIndex(conf, path);
    SamReaderFactory readerFactory = SamReaderFactory.makeDefault()
        .setOption(SamReaderFactory.Option.CACHE_FILE_BASED_INDEXES, true)
        .setOption(SamReaderFactory.Option.EAGERLY_DECODE, false)
        .setUseAsyncIo(false);
    if (stringency != null) {
      readerFactory.validationStringency(stringency);
    }
    if (referenceSourcePath != null) {
      // TODO: should go through FileSystemWrapper
      readerFactory.referenceSource(new ReferenceSource(NIOFileUtil.asPath(referenceSourcePath)));
    }
    SamInputResource resource = SamInputResource.of(in);
//    if (indexStream != null) {
//      resource.index(indexStream);
//    }
    return readerFactory.open(resource);
  }
}
