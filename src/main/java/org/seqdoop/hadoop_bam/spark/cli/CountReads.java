package org.seqdoop.hadoop_bam.spark.cli;

import htsjdk.samtools.ValidationStringency;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.spark.BamSource;
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
      jsc.hadoopConfiguration().set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.name());
      return new BamSource(false).getReads(jsc, path, splitSize).count();
    }
  }

  public static long countReads(String path, String sparkMaster, int splitSize) throws IOException {
    try (JavaSparkContext jsc = new JavaSparkContext(sparkMaster, "CountReads")) {
      jsc.hadoopConfiguration().set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.name());
      return new BamSource().getReads(jsc, path, splitSize).count();
    }
  }

  public static void main(String... args) throws IOException {
    if (args.length != 4) {
      System.err.println("Usage: CountReads <mode> <BAM file> <spark master> <split size>");
      System.exit(1);
    }
    String mode = args[0];
    String path = args[1];
    String sparkMaster = args[2];
    int splitSize = Integer.parseInt(args[3]);
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
        System.out.println(countReads(path, sparkMaster, splitSize));
        break;
      }
    }
  }
}
