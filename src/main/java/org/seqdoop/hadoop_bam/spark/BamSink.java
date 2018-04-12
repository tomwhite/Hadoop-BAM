package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * Write reads to a BAM file on Spark.
 *
 * @see BamSource
 * @see SamDataset
 */
class BamSink {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  public void save(JavaSparkContext jsc, SAMFileHeader header, JavaRDD<SAMRecord> reads,
      String path) throws IOException {

    String shardedDir = path + ".sharded";
    Broadcast<SAMFileHeader> headerBroadcast = jsc.broadcast(header);
    reads.mapPartitions(readIterator -> {
          BamOutputFormat.setHeader(headerBroadcast.getValue());
          return readIterator;
        })
        .mapToPair(
            (PairFunction<SAMRecord, Void, SAMRecord>) samRecord -> new Tuple2<>(null, samRecord))
        .saveAsNewAPIHadoopFile(shardedDir, Void.class, SAMRecord.class, BamOutputFormat.class, jsc.hadoopConfiguration());

    String headerFile = shardedDir + "/header";
    try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), headerFile)) {
      writeHeader(header, out);
    }

    String terminatorFile = shardedDir + "/terminator";
    try (OutputStream out = fileSystemWrapper.create(jsc.hadoopConfiguration(), terminatorFile)) {
      out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
    }

    new Merger().mergeParts(jsc.hadoopConfiguration(), shardedDir, path);
  }

  private void writeHeader(SAMFileHeader header, OutputStream out) throws IOException {
    // TODO: this is copied from htsjdk BAMFileWriter#writeHeader, which is protected.
    final StringWriter headerTextBuffer = new StringWriter();
    new SAMTextHeaderCodec().encode(headerTextBuffer, header);
    final String headerText = headerTextBuffer.toString();

    BlockCompressedOutputStream blockCompressedOutputStream = new BlockCompressedOutputStream(out, null);
    BinaryCodec outputBinaryCodec = new BinaryCodec(blockCompressedOutputStream);
    outputBinaryCodec.writeBytes("BAM\1".getBytes());

    // calculate and write the length of the SAM file header text and the header text
    outputBinaryCodec.writeString(headerText, true, false);

    // write the sequences binarily.  This is redundant with the text header
    outputBinaryCodec.writeInt(header.getSequenceDictionary().size());
    for (final SAMSequenceRecord sequenceRecord: header.getSequenceDictionary().getSequences()) {
      outputBinaryCodec.writeString(sequenceRecord.getSequenceName(), true, true);
      outputBinaryCodec.writeInt(sequenceRecord.getSequenceLength());
    }

    outputBinaryCodec.getOutputStream().flush(); // don't close BlockCompressedOutputStream since we don't want to write the terminator
  }
}
