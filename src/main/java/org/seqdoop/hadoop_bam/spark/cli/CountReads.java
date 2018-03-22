package org.seqdoop.hadoop_bam.spark.cli;

import htsjdk.samtools.ValidationStringency;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.spark.SamDatasetFactory;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

public class CountReads {

  public static long countReadsLegacy(String path, String sparkMaster, int splitSize) throws IOException {
    try (JavaSparkContext jsc = new JavaSparkContext(sparkMaster, "CountReadsLegacy")) {
      jsc.hadoopConfiguration().set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.name());
      return jsc.newAPIHadoopFile(
          path, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class,
          jsc.hadoopConfiguration()).count();
    }
  }

  public static long countReadsNio(String path, String sparkMaster, int splitSize) throws IOException {
    try (JavaSparkContext jsc = new JavaSparkContext(sparkMaster, "CountReadsNio")) {
      return SamDatasetFactory.makeDefault(jsc)
          .splitSize(splitSize)
          .validationStringency(ValidationStringency.SILENT)
          .useNio(true)
          .read(path)
          .getReadsRdd()
          .count();
    }
  }

  public static long countReads(String path, String sparkMaster, int splitSize, String cramReferenceSource) throws IOException {
    try (JavaSparkContext jsc = new JavaSparkContext(sparkMaster, "CountReads")) {
      SamDatasetFactory samDatasetFactory = SamDatasetFactory.makeDefault(jsc)
          .splitSize(splitSize)
          .validationStringency(ValidationStringency.SILENT);
      if (cramReferenceSource != null) {
        samDatasetFactory.referenceSourcePath(cramReferenceSource);
      }
      return samDatasetFactory
          .read(path)
          .getReadsRdd()
          .count();
    }
  }

  public static void main(String... args) throws IOException {
    if (args.length != 4 && args.length != 5) {
      System.err.println("Usage: CountReads <mode> <BAM file> <spark master> <split size> [CRAM reference source]");
      System.exit(1);
    }
    String mode = args[0];
    String path = args[1];
    String sparkMaster = args[2];
    int splitSize = Integer.parseInt(args[3]);
    String cramReferenceSource = args.length == 4 ? null : args[4];
    switch (mode) {
      case "legacy": {
        System.out.println(countReadsLegacy(path, sparkMaster, splitSize));
        break;
      }
      case "nio": {
        System.out.println(countReadsNio(path, sparkMaster, splitSize));
        break;
      }
      default: {
        System.out.println(countReads(path, sparkMaster, splitSize, cramReferenceSource));
        break;
      }
    }
  }
}
