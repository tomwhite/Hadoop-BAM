package org.seqdoop.hadoop_bam.spark;

import com.google.common.collect.Iterators;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderVersion;
import java.io.IOException;
import java.io.OutputStream;
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
import org.seqdoop.hadoop_bam.util.BGZFCodec;
import org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;

public class VcfDatasetFactory {

  private JavaSparkContext sparkContext;
  private int splitSize;

  public static VcfDatasetFactory makeDefault(JavaSparkContext sparkContext) {
    return new VcfDatasetFactory(sparkContext);
  }

  private VcfDatasetFactory(JavaSparkContext sparkContext) {
    this.sparkContext = sparkContext;
  }

  public VcfDatasetFactory splitSize(int splitSize) {
    this.splitSize = splitSize;
    return this;
  }

  public VcfDataset read(String path) throws IOException {
    // Use Hadoop FileSystem API to maintain file locality by using Hadoop's FileInputFormat
    FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

    final Configuration conf = sparkContext.hadoopConfiguration();
    if (splitSize > 0) {
      conf.setInt(FileInputFormat.SPLIT_MAXSIZE, splitSize);
    }
    enableBGZFEnhancedGzipCodec(conf);

    try (SeekableStream headerIn = fileSystemWrapper.open(conf, path)) {
      VCFHeader header = VCFHeaderReader.readHeaderFrom(headerIn);
      Broadcast<VCFHeader> vcfHeaderBroadcast = sparkContext.broadcast(header);

      JavaRDD<VariantContext> variants = textFile(sparkContext, path)
          .mapPartitions((FlatMapFunction<Iterator<String>, VariantContext>) lines -> {
            VCFCodec codec = new VCFCodec(); // Use map partitions so we can reuse codec (not broadcast-able)
            codec.setVCFHeader(vcfHeaderBroadcast.getValue(), VCFHeaderVersion.VCF4_1); // TODO: how to determine version?
            return Iterators.transform(Iterators.filter(lines, line -> !line.startsWith("#")), codec::decode);
          });
      return new VcfDataset(header, variants);
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

  public void write(VcfDataset vcfDataset, String path) throws IOException {
    FileSystemWrapper fileSystemWrapper = new HadoopFileSystemWrapper();

    String shardedDir = path + ".sharded";
    Broadcast<VCFHeader> vcfHeaderBroadcast = sparkContext.broadcast(vcfDataset.getFileHeader());
    JavaRDD<String> variantStrings = vcfDataset.getVariantsRdd()
        .mapPartitions((FlatMapFunction<Iterator<VariantContext>, String>) variantContexts -> {
          VCFEncoder vcfEncoder = new VCFEncoder(vcfHeaderBroadcast.getValue(), false, false);
          return Iterators.transform(variantContexts, vcfEncoder::encode);
        });
    boolean compressed = path.endsWith(BGZFCodec.DEFAULT_EXTENSION) || path.endsWith(".gz");
    if (compressed) {
      variantStrings.saveAsTextFile(shardedDir, BGZFCodec.class);
    } else {
      variantStrings.saveAsTextFile(shardedDir);
    }
    String headerFile = shardedDir + "/header" + (compressed ? BGZFCodec.DEFAULT_EXTENSION : "");
    try (OutputStream headerOut = fileSystemWrapper.create(sparkContext.hadoopConfiguration(), headerFile)) {
      OutputStream out = compressed ? new BlockCompressedOutputStream(headerOut, null) : headerOut;
      VariantContextWriter writer = new VariantContextWriterBuilder().clearOptions()
          .setOutputVCFStream(out).build();
      writer.writeHeader(vcfDataset.getFileHeader());
      out.flush(); // don't close BlockCompressedOutputStream since we don't want to write the terminator after the header
    }
    if (compressed) {
      String terminatorFile = shardedDir + "/terminator";
      try (OutputStream out = fileSystemWrapper.create(sparkContext.hadoopConfiguration(), terminatorFile)) {
        out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
      }
    }
    new Merger().mergeParts(sparkContext.hadoopConfiguration(), shardedDir, path);
  }

}
