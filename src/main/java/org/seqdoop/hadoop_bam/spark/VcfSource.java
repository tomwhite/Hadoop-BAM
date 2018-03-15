package org.seqdoop.hadoop_bam.spark;

import com.google.common.collect.Iterators;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;

public class VcfSource implements Serializable {

  private FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

  public JavaRDD<VariantContext> getVariants(JavaSparkContext jsc, String path, int splitSize)
      throws IOException {

    // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat

    final Configuration conf = jsc.hadoopConfiguration();
    if (splitSize > 0) {
      conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
    }
    enableBGZFEnhancedGzipCodec(conf);

    try (SeekableStream headerIn = fileSystemWrapper.open(conf, path)) {
      VCFHeader vcfHeader = VCFHeaderReader.readHeaderFrom(headerIn);
      Broadcast<VCFHeader> vcfHeaderBroadcast = jsc.broadcast(vcfHeader);

      return textFile(jsc, path)
          .mapPartitions((FlatMapFunction<Iterator<String>, VariantContext>) lines -> {
            VCFCodec codec = new VCFCodec(); // Use map partitions so we can reuse codec (not broadcast-able)
            codec.setVCFHeader(vcfHeaderBroadcast.getValue(), VCFHeaderVersion.VCF4_1); // TODO: how to determine version?
            return Iterators.transform(Iterators.filter(lines, line -> !line.startsWith("#")), codec::decode);
          });
    }
  }

  private void enableBGZFEnhancedGzipCodec(Configuration conf) {
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
    codecs.remove(GzipCodec.class);
    codecs.add(BGZFEnhancedGzipCodec.class);
    CompressionCodecFactory.setCodecClasses(conf, new ArrayList<>(codecs));
  }

  private JavaRDD<String> textFile(JavaSparkContext jsc, String path) {
    // Use this over JavaSparkContext#textFile since this allows the configuration to be passed in
    return jsc.newAPIHadoopFile(path, TextInputFormat.class, LongWritable.class, Text.class, jsc.hadoopConfiguration())
        .map(pair -> pair._2.toString())
        .setName(path);
  }
}
