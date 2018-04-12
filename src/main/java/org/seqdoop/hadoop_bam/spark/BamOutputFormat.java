package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.BAMRecordCodec;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.util.BinaryCodec;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p>
 * An output format for writing {@link SAMRecord} objects to BAM files. Should not be used directly.
 * </p>
 *
 * @see SamDataset
 */
public class BamOutputFormat extends FileOutputFormat<Void, SAMRecord> {

  static class BamRecordWriter extends RecordWriter<Void, SAMRecord> {

    private final OutputStream out;
    private final BinaryCodec binaryCodec;
    private final BAMRecordCodec bamRecordCodec;

    public BamRecordWriter(Configuration conf, Path file, SAMFileHeader header) throws IOException {
      this.out = file.getFileSystem(conf).create(file);
      BlockCompressedOutputStream compressedOut = new BlockCompressedOutputStream(out, null);
      binaryCodec = new BinaryCodec(compressedOut);
      bamRecordCodec = new BAMRecordCodec(header);
      bamRecordCodec.setOutputStream(compressedOut);
    }

    @Override
    public void write(Void ignore, SAMRecord samRecord) {
      bamRecordCodec.encode(samRecord);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext)
        throws IOException {
      binaryCodec.getOutputStream().flush();
      out.close(); // don't close BlockCompressedOutputStream since we don't want to write the terminator
    }
  }

  private static SAMFileHeader header;

  public static void setHeader(SAMFileHeader samFileHeader) {
    header = samFileHeader;
  }

  @Override
  public RecordWriter<Void, SAMRecord> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Path file = getDefaultWorkFile(taskAttemptContext, "");
    return new BamRecordWriter(taskAttemptContext.getConfiguration(), file, header);
  }
}
