package org.seqdoop.hadoop_bam.spark;

import com.google.common.collect.Iterators;
import htsjdk.samtools.AbstractBAMFileIndex;
import htsjdk.samtools.BAMFileReader;
import htsjdk.samtools.BAMFileSpan;
import htsjdk.samtools.BAMIndex;
import htsjdk.samtools.Chunk;
import htsjdk.samtools.QueryInterval;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReader.PrimitiveSamReaderToSamReaderAdapter;
import htsjdk.samtools.SamReaderFactory;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.seqdoop.hadoop_bam.spark.BgzfBlockGuesser.BgzfBlock;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import scala.Tuple2;

/**
 * Load reads from a BAM file on Spark.
 *
 * @see BamSink
 */
class BamSource implements Serializable {

  private static final int MAX_READ_SIZE = 10_000_000;

  private final BgzfBlockSource bgzfBlockSource;
  private final FileSystemWrapper fileSystemWrapper;

  public BamSource() {
    this(false);
  }

  /**
   * @param useNio if true use the NIO filesystem APIs rather than the Hadoop filesystem APIs.
   * This is appropriate for cloud stores where file locality is not relied upon.
   */
  public BamSource(boolean useNio) {
    this.bgzfBlockSource = new BgzfBlockSource(useNio);
    this.fileSystemWrapper = useNio ? new NioFileSystemWrapper() : new HadoopFileSystemWrapper();
  }

  public SAMFileHeader getFileHeader(JavaSparkContext jsc, String path) throws IOException {
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
    try (InputStream headerIn = fileSystemWrapper.open(conf, firstBamPath)) {
      return SAMHeaderReader.readSAMHeaderFrom(headerIn, conf);
    }
  }

  /**
   * @return an RDD for the first read starting in each partition, or null for partitions
   * that don't have a read starting in them.
   */
  <T extends Locatable> JavaRDD<ReadStart> getReadStarts(JavaSparkContext jsc, String path, int splitSize, Broadcast<TraversalParameters<T>> traversalParametersBroadcast, ValidationStringency stringency)
      throws IOException {
    Configuration conf = jsc.hadoopConfiguration();
    SerializableHadoopConfiguration confSer = new SerializableHadoopConfiguration(conf);
    return bgzfBlockSource.getBgzfBlocks(jsc, path, splitSize)
        .mapPartitions((FlatMapFunction<Iterator<BgzfBlock>, ReadStart>) bgzfBlocks -> Collections
            .singletonList(getFirstReadInPartition(confSer.getConf(), bgzfBlocks, traversalParametersBroadcast, stringency)).iterator());
  }

  /**
   * @return the first read starting in the partition, or null if there is none (e.g. in the case
   * of long reads, and/or very small partitions).
   */
  private <T extends Locatable> ReadStart getFirstReadInPartition(Configuration conf, Iterator<BgzfBlock> bgzfBlocks, Broadcast<TraversalParameters<T>> traversalParametersBroadcast, ValidationStringency stringency)
      throws IOException {
    String path = null;
    BamRecordGuesser bamRecordGuesser = null;
    try {
      BAMFileSpan span = null;
      long unplacedUnmappedStart = -1;
      int index = 0; // limit search to MAX_READ_SIZE positions
      while (bgzfBlocks.hasNext()) {
        BgzfBlock block = bgzfBlocks.next();
        if (path == null) { // assume each partition comes from only a single file path
          path = block.path;
          try (InputStream headerIn = fileSystemWrapper.open(conf, path)) {
            SAMFileHeader header = SAMHeaderReader.readSAMHeaderFrom(headerIn, conf);
            bamRecordGuesser = getBamRecordGuesser(conf, path, header);
            if (traversalParametersBroadcast != null) {
              try (SamReader samReader = createSamReader(conf, path, stringency)) {
                if (!samReader.hasIndex()) {
                  throw new IllegalArgumentException(
                      "Intervals set but no BAM index file found for " + path);

                }
                SAMSequenceDictionary dict = header.getSequenceDictionary();
                BAMIndex idx = samReader.indexing().getIndex();
                TraversalParameters<T> traversalParameters = traversalParametersBroadcast
                    .getValue();
                if (traversalParameters.getIntervalsForTraversal() != null) {
                  QueryInterval[] queryIntervals = BoundedTraversalUtil
                      .prepareQueryIntervals(traversalParameters.getIntervalsForTraversal(), dict);
                  span = BAMFileReader.getFileSpan(queryIntervals, idx);
                }
                if (traversalParameters.getTraverseUnplacedUnmapped()) {
                  long startOfLastLinearBin = idx.getStartOfLastLinearBin();
                  long noCoordinateCount = ((AbstractBAMFileIndex) idx).getNoCoordinateCount();
                  if (startOfLastLinearBin != -1 && noCoordinateCount > 0) {
                    unplacedUnmappedStart = startOfLastLinearBin;
                  }
                }
              }
            }
          }
        }
        for (int up = 0; up < block.uSize; up++) {
          index++;
          if (index > MAX_READ_SIZE) {
            return null;
          }
          long vPos = block.pos << 16 | (long) up;
          if (bamRecordGuesser.checkRecordStart(vPos)) {
            block.end();
            return new ReadStart(vPos, path, span, unplacedUnmappedStart);
          }
        }
      }
    } finally {
      if (bamRecordGuesser != null) {
        bamRecordGuesser.close();
      }
    }
    return null; // no read found
  }

  private BamRecordGuesser getBamRecordGuesser(Configuration conf, String path, SAMFileHeader header) throws IOException {
    SeekableStream ss = new SeekableBufferedStream(fileSystemWrapper.open(conf, path));
    return new BamRecordGuesser(ss, header.getSequenceDictionary().size(), header);
  }

  /**
   * @return an RDD of reads.
   */
  public JavaRDD<SAMRecord> getReads(JavaSparkContext jsc, String path, int splitSize, ValidationStringency stringency) throws IOException {
    return getReads(jsc, path, splitSize, null, stringency);
  }

  /**
   * @return an RDD of reads for a bounded traversal (intervals and whether to return unplaced, unmapped reads).
   */
  public <T extends Locatable> JavaRDD<SAMRecord> getReads(JavaSparkContext jsc, String path, int splitSize, TraversalParameters<T> traversalParameters, ValidationStringency stringency) throws IOException {
    if (traversalParameters != null && traversalParameters.getIntervalsForTraversal() == null && !traversalParameters.getTraverseUnplacedUnmapped()) {
      throw new IllegalArgumentException("Traversing mapped reads only is not supported.");
    }

    Broadcast<TraversalParameters<T>> traversalParametersBroadcast = traversalParameters == null ? null : jsc.broadcast(traversalParameters);
    JavaRDD<ReadStart> readStartsRdd = getReadStarts(jsc, path, splitSize, traversalParametersBroadcast, stringency);
    List<ReadStart> readStarts = readStartsRdd.collect();
    // Find the end of the partition, which is either the start of the read in the next partition,
    // or the end of the file if a partition is the last for a file.
    List<Chunk> readRanges = new ArrayList<>();
    for (int i = 0; i < readStarts.size(); i++) {
      ReadStart readStart = readStarts.get(i);
      Chunk readRange;
      if (readStart == null) {
        readRange = null;
      } else {
        long readStartPos = readStart.getVirtualStart();
        long readEnd = -1;
        // if there's another read start in the same file, use it as end (otherwise set it as file end)
        for (int j = i + 1; j < readStarts.size(); j++) {
          ReadStart nextReadStart = readStarts.get(j);
          if (nextReadStart == null) {
            continue;
          }
          if (readStart.getPath().equals(nextReadStart.getPath())) {
            readEnd = nextReadStart.getVirtualStart();
          }
          break;
        }
        if (readEnd == 0) {
          readEnd = fileSystemWrapper.getFileLength(jsc.hadoopConfiguration(), readStart.getPath()) << 16;
        }
        readRange = new Chunk(readStartPos, readEnd);
      }
      readRanges.add(readRange);
    }

    SerializableHadoopConfiguration confSer = new SerializableHadoopConfiguration(jsc.hadoopConfiguration());

    JavaRDD<Chunk> readRangesRdd = jsc.parallelize(readRanges, readRanges.size());
    return readStartsRdd
        .zip(readRangesRdd) // one element per partition
        .mapPartitions((FlatMapFunction<Iterator<Tuple2<ReadStart, Chunk>>, SAMRecord>) it -> {
          Tuple2<ReadStart, Chunk> tuple = it.next();
          ReadStart readStart = tuple._1();
          Chunk readRange = tuple._2();
          if (readRange == null) {
            return Collections.emptyIterator();
          }
          Configuration conf = confSer.getConf();
          String p = readStart.getPath();
          SamReader samReader = createSamReader(conf, p, stringency);
          BAMFileReader bamFileReader = createBamFileReader(samReader);
          BAMFileSpan splitSpan = new BAMFileSpan(readRange);
          BAMFileSpan span = readStart.getSpan();
          TraversalParameters<T> traversal = traversalParametersBroadcast == null ? null : traversalParametersBroadcast.getValue();
          if (traversal != null) {
            Iterator<SAMRecord> intervalReadsIterator;
            if (traversal.getIntervalsForTraversal() == null) {
              intervalReadsIterator = Collections.emptyIterator();
              samReader.close(); // not needed
            } else {
              span = (BAMFileSpan) span.removeContentsBefore(splitSpan);
              span = (BAMFileSpan) span.removeContentsAfter(splitSpan);
              // TODO: share QueryInterval
              QueryInterval[] queryIntervals = createQueryIntervals(conf, p,
                  traversal.getIntervalsForTraversal());
              intervalReadsIterator = new AutocloseIteratorWrapper<>(
                  bamFileReader.createIndexIterator(queryIntervals, false, span.toCoordinateArray()),
                  samReader);
            }

            // add on unplaced unmapped reads if there are any in this range
            if (traversal.getTraverseUnplacedUnmapped() && readStart.getUnplacedUnmappedStart() != -1 &&
                readRange.getChunkStart() <= readStart.getUnplacedUnmappedStart() &&
                readStart.getUnplacedUnmappedStart() < readRange.getChunkEnd()) { // TODO correct?
              SamReader unplacedUnmappedReadsSamReader = createSamReader(conf, p, stringency);
              Iterator<SAMRecord> unplacedUnmappedReadsIterator = new AutocloseIteratorWrapper<>(
                  createBamFileReader(unplacedUnmappedReadsSamReader).queryUnmapped(),
                  unplacedUnmappedReadsSamReader);
              return Iterators.concat(intervalReadsIterator, unplacedUnmappedReadsIterator);
            }
            return intervalReadsIterator;
          } else {
            return new AutocloseIteratorWrapper<>(bamFileReader.getIterator(splitSpan), samReader);
          }
        });
  }

  private BAMFileReader createBamFileReader(SamReader samReader) {
    BAMFileReader bamFileReader = (BAMFileReader) ((PrimitiveSamReaderToSamReaderAdapter) samReader).underlyingReader();
    if (bamFileReader.hasIndex()) {
      bamFileReader.getIndex(); // force BAMFileReader#mIndex to be populated so the index stream is properly closed by the close() method
    }
    return bamFileReader;
  }

  private SeekableStream findIndex(Configuration conf, String path) throws IOException {
    String index = path + ".bai";
    if (fileSystemWrapper.exists(conf, index)) {
      return fileSystemWrapper.open(conf, index);
    }
    index = path.replaceFirst("\\.bam$", ".bai");
    if (fileSystemWrapper.exists(conf, index)) {
      return fileSystemWrapper.open(conf, index);
    }
    return null;
  }

  private SamReader createSamReader(Configuration conf, String path, ValidationStringency stringency) throws IOException {
    SeekableStream in = fileSystemWrapper.open(conf, path);
    SeekableStream indexStream = findIndex(conf, path);
    SamReaderFactory readerFactory = SamReaderFactory.makeDefault()
        .setOption(SamReaderFactory.Option.CACHE_FILE_BASED_INDEXES, true)
        .setOption(SamReaderFactory.Option.EAGERLY_DECODE, false)
        .setUseAsyncIo(false);
    if (stringency != null) {
      readerFactory.validationStringency(stringency);
    }
    SamInputResource resource = SamInputResource.of(in);
    if (indexStream != null) {
      resource.index(indexStream);
    }
    return readerFactory.open(resource);
  }

  private <T extends Locatable> QueryInterval[] createQueryIntervals(Configuration conf, String path, List<T> intervals) throws IOException {
    try (InputStream headerIn = fileSystemWrapper.open(conf, path)) {
      SAMFileHeader header = SAMHeaderReader.readSAMHeaderFrom(headerIn, conf);
      SAMSequenceDictionary dict = header.getSequenceDictionary();
      return BoundedTraversalUtil.prepareQueryIntervals(intervals, dict);
    }
  }

  /**
   * Stores the position of the first BAM record found in a partition, along with information to
   * support bounded traversal (intervals and whether to return unplaced, unmapped reads).
   */
  static class ReadStart implements Serializable {
    private final long virtualStart;
    private final String path;
    private final BAMFileSpan span;
    private long unplacedUnmappedStart;

    public ReadStart(long virtualStart, String path, BAMFileSpan span, long unplacedUnmappedStart) {
      this.virtualStart = virtualStart;
      this.path = path;
      this.span = span;
      this.unplacedUnmappedStart = unplacedUnmappedStart;
    }

    public long getVirtualStart() {
      return virtualStart;
    }

    public String getPath() {
      return path;
    }

    public BAMFileSpan getSpan() {
      return span;
    }

    public long getUnplacedUnmappedStart() {
      return unplacedUnmappedStart;
    }
  }
}
