package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.cram.build.CramIO;
import htsjdk.samtools.util.Locatable;
import java.io.IOException;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SamDatasetFactory {

  private JavaSparkContext sparkContext;
  private int splitSize;
  private ValidationStringency validationStringency = ValidationStringency.DEFAULT_STRINGENCY;
  private boolean useNio;
  private String referenceSourcePath;

  public static SamDatasetFactory makeDefault(JavaSparkContext sparkContext) {
    return new SamDatasetFactory(sparkContext);
  }

  private SamDatasetFactory(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  public SamDatasetFactory splitSize(int splitSize) {
    this.splitSize = splitSize;
    return this;
  }

  public SamDatasetFactory validationStringency(ValidationStringency validationStringency) {
    this.validationStringency = validationStringency;
    return this;
  }

  public SamDatasetFactory useNio(boolean useNio) {
    this.useNio = useNio;
    return this;
  }

  public SamDatasetFactory referenceSourcePath(String referenceSourcePath) {
    this.referenceSourcePath = referenceSourcePath;
    return this;
  }

  public SamDataset read(String path) throws IOException {
    if (path.endsWith(CramIO.CRAM_FILE_EXTENSION)) {
      CramSource cramSource = new CramSource();
      SAMFileHeader header = cramSource.getFileHeader(sparkContext, path, validationStringency, referenceSourcePath);
      JavaRDD<SAMRecord> reads = cramSource.getReads(sparkContext, path, splitSize, validationStringency, referenceSourcePath);
      return new SamDataset(header, reads);
    } else {
      BamSource bamSource = new BamSource(useNio);
      SAMFileHeader header = bamSource.getFileHeader(sparkContext, path);
      JavaRDD<SAMRecord> reads = bamSource.getReads(sparkContext, path, splitSize, validationStringency);
      return new SamDataset(header, reads);
    }
  }

  public <T extends Locatable> SamDataset read(String path, List<T> intervals, boolean traverseUnplacedUnmapped) throws IOException {
    BamSource bamSource = new BamSource(useNio);
    SAMFileHeader header = bamSource.getFileHeader(sparkContext, path);
    JavaRDD<SAMRecord> reads = bamSource.getReads(sparkContext, path, splitSize, intervals, traverseUnplacedUnmapped, validationStringency);
    return new SamDataset(header, reads);
  }

  public void write(SamDataset samDataset, String path) throws IOException {
    new BamSink().save(sparkContext, samDataset.getFileHeader(), samDataset.getReadsRdd(), path);
  }

}
