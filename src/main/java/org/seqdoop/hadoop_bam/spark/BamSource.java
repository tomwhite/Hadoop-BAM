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
   * @return the {@link ReadRange} for the partition, or null if there is none (e.g. in the case
   * of long reads, and/or very small partitions).
   */
  private <T extends Locatable> ReadRange getFirstReadInPartition(Configuration conf, Iterator<BgzfBlock> bgzfBlocks)
      throws IOException {
    ReadRange readRange = null;
    BamRecordGuesser bamRecordGuesser = null;
    try {
      String partitionPath = null;
      int index = 0; // limit search to MAX_READ_SIZE positions
      outer:
      while (bgzfBlocks.hasNext()) {
        BgzfBlock block = bgzfBlocks.next();
        if (partitionPath == null) { // assume each partition comes from only a single file path
          partitionPath = block.path;
          try (InputStream headerIn = fileSystemWrapper.open(conf, partitionPath)) {
            SAMFileHeader header = SAMHeaderReader.readSAMHeaderFrom(headerIn, conf);
            bamRecordGuesser = getBamRecordGuesser(conf, partitionPath, header);
          }
        }
        for (int up = 0; up < block.uSize; up++) {
          index++;
          if (index > MAX_READ_SIZE) {
            return null;
          }
          long vPos = block.pos << 16 | (long) up;
          long vEnd = block.end << 16;
          if (bamRecordGuesser.checkRecordStart(vPos)) {
            block.end();
            return new ReadRange(partitionPath, new Chunk(vPos, vEnd));
          }
        }
      }
    } finally {
      if (bamRecordGuesser != null) {
        bamRecordGuesser.close();
      }
    }
    return readRange;
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
    SerializableHadoopConfiguration confSer = new SerializableHadoopConfiguration(jsc.hadoopConfiguration());

    return bgzfBlockSource.getBgzfBlocks(jsc, path, splitSize)
        .mapPartitions((FlatMapFunction<Iterator<BgzfBlock>, SAMRecord>) bgzfBlocks -> {
          Configuration conf = confSer.getConf();
          ReadRange readRange = getFirstReadInPartition(conf, bgzfBlocks);
          if (readRange == null) {
            return Collections.emptyIterator();
          }
          String p = readRange.getPath();
          SamReader samReader = createSamReader(conf, p, stringency);
          BAMFileReader bamFileReader = createBamFileReader(samReader);
          BAMFileSpan splitSpan = new BAMFileSpan(readRange.getSpan());
          TraversalParameters<T> traversal = traversalParametersBroadcast == null ? null : traversalParametersBroadcast.getValue();
          if (traversal == null) {
            // no intervals or unplaced, unmapped reads
            return new AutocloseIteratorWrapper<>(bamFileReader.getIterator(splitSpan), samReader);
          } else {
            if (!samReader.hasIndex()) {
              throw new IllegalArgumentException(
                  "Intervals set but no BAM index file found for " + p);

            }
            BAMIndex idx = samReader.indexing().getIndex();
            Iterator<SAMRecord> intervalReadsIterator;
            if (traversal.getIntervalsForTraversal() == null) {
              intervalReadsIterator = Collections.emptyIterator();
            } else {
              QueryInterval[] queryIntervals = createQueryIntervals(conf, p,
                  traversal.getIntervalsForTraversal());
              BAMFileSpan span = BAMFileReader.getFileSpan(queryIntervals, idx);
              span = (BAMFileSpan) span.removeContentsBefore(splitSpan);
              span = (BAMFileSpan) span.removeContentsAfter(splitSpan);
              intervalReadsIterator = new AutocloseIteratorWrapper<>(
                  bamFileReader.createIndexIterator(queryIntervals, false, span.toCoordinateArray()),
                  samReader);
            }

            // add on unplaced unmapped reads if there are any in this range
            if (traversal.getTraverseUnplacedUnmapped()) {
              long startOfLastLinearBin = idx.getStartOfLastLinearBin();
              long noCoordinateCount = ((AbstractBAMFileIndex) idx).getNoCoordinateCount();
              if (startOfLastLinearBin != -1 && noCoordinateCount > 0) {
                long unplacedUnmappedStart = startOfLastLinearBin;
                if (readRange.getSpan().getChunkStart() <= unplacedUnmappedStart &&
                    unplacedUnmappedStart < readRange.getSpan().getChunkEnd()) { // TODO correct?
                  SamReader unplacedUnmappedReadsSamReader = createSamReader(conf, p, stringency);
                  Iterator<SAMRecord> unplacedUnmappedReadsIterator = new AutocloseIteratorWrapper<>(
                      createBamFileReader(unplacedUnmappedReadsSamReader).queryUnmapped(),
                      unplacedUnmappedReadsSamReader);
                  return Iterators.concat(intervalReadsIterator, unplacedUnmappedReadsIterator);
                }
              }
              if (traversal.getIntervalsForTraversal() == null) {
                samReader.close(); // not used any more
              }
            }
            return intervalReadsIterator;
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
   * Stores the virtual span of a partition, from the start of the first read, to the end of the partition.
   */
  static class ReadRange implements Serializable {
    private final String path;
    private final Chunk span;
    public ReadRange(String path, Chunk span) {
      this.path = path;
      this.span = span;
    }

    public String getPath() {
      return path;
    }

    public Chunk getSpan() {
      return span;
    }
  }
}
