package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.spark.api.java.JavaRDD;

/**
 * <p>
 * A {@link SamDataset} is the distributed equivalent of a htsjdk {@link htsjdk.samtools.SamReader}.
 * It represents a SAM, BAM, or CRAM file stored in a distributed filesystem (although only BAM is
 * supported at the moment), and encapsulates a Spark RDD containing the reads in it.
 * </p>
 * <p>
 * Use a {@link SamDatasetFactory} to read and write {@link SamDataset}s.
 * </p>
 * @see SamDatasetFactory
 */
public class SamDataset {

  private final SAMFileHeader header;
  private final JavaRDD<SAMRecord> reads;

  public SamDataset(SAMFileHeader header,
      JavaRDD<SAMRecord> reads) {
    this.header = header;
    this.reads = reads;
  }

  public SAMFileHeader getFileHeader() {
    return header;
  }

  public JavaRDD<SAMRecord> getReadsRdd() {
    return reads;
  }
}
