# Spark API

A re-implementation of Hadoop-BAM for Apache Spark.

## Motivation

[Spark-BAM](http://www.hammerlab.org/spark-bam/) has shown that reading BAMs for Spark can be more
correct and more performant than the Hadoop-BAM implementation. Furthermore, all known users of
Hadoop-BAM are using Spark, not MapReduce, as their processing engine so it makes a lot of sense to
target the Spark API, which gives us higher-level primitives than raw MR (.e.g broadcasts).

## Hosting and Governance

One option would be to add the new implementation to the existing Hadoop-BAM project. There is
actually very little overlap between Hadoop-BAM and the new implementation code (`BGZFCodec` is the
main exception here), so an alternative would be to start a new project and deprecate Hadoop-BAM.

Where would the new project live? One proposal is as a
[samtools project on GitHub](https://github.com/samtools), like
[htsjdk](https://github.com/samtools/htsjdk) but for Spark. Indeed, the new API leans heavily on
htsjdk for implementation and API classes, so it would naturally sit nearby that project.

Following [Apache Software Foundation
policy](https://www.apache.org/foundation/marks/faq/#products), the project name cannot include
the word "Spark". One candidate: _htsrdd_.

## Scope

The project scope is to provide a means to manipulate files in common bioinformatics formats from
Spark.

## Features

The following discusses the features and level of support provided by the library.

### Formats

The library should be able to read and write BAM and VCF formats, at a minimum. More formats
will be added over time, as needed.

Formats are converted into Spark RDDs using htsjdk types: `SAMRecord` (for BAM) and
`VariantContext` (for VCF).

### Filesystems

Two filesystem abstractions are supported for all formats: the Hadoop filesystem (HDFS, local,
and others such as S3), and Java NIO filesystems (local, S3, GCS, etc).

Only one filesystem abstraction is used for each operation (unlike current Hadoop-BAM, which 
mixes the two, e.g. using Hadoop for bulk loading, and the HDFS NIO plugin for metadata
operations). The choice of which to use (Hadoop vs. NIO) is set by the user. Roughly speaking,
Hadoop is best for HDFS clusters (including those running in the cloud), and NIO is appropriate
for cloud stores.

### Compression

For BAM, compression is a part of the file format, so it is necessarily supported.

For reading VCF, support includes
[BGZF](https://samtools.github.io/hts-specs/SAMv1.pdf)-compressed (`.vcf.bgz` or `.vcf.gz`) and
gzip-compressed files (`.vcf.gz`).

For writing VCF, only BGZF-compressed files can be written (gzip
is not splittable so it is a mistake to write this format).

### Multiple input files

For reading BAM and VCF, multiple files may be read in one operation. The input paths may be a
list of individual files, directories, or a mixture of the two. Directories are _not_ processed
recursively, so only the files in the directory are processed, and it is an error for the
directory to contain subdirectories.

File types may not be mixed: it is an error to process BAM and CRAM files, for example, in one
operation.

When multiple files are read in one operation their headers are merged.

### Sharded output

For writing BAM and VCF, by default whole single files are written, but the output files may
optionally be sharded, for efficiency. A sharded BAM file has the following directory structure:

```
.
└── output.bam.sharded/
    ├── header
    ├── part-00000
    ├── part-00001
    ├── ...
    ├── part-00009
    └── terminator

```

Note that `output.bam.sharded` is a directory and contains files, which can be concatenated
to form a valid BAM. A similar structure is used for VCF. (Note that it is not always possible
to concatenate shards together for some file or index formats, e.g. BAI or CRAM.)

Sharded files are treated as a single file for the purposes of reading multiple inputs.

### Indexes

For reading BAM, if there is no index, then the file is split using a heuristic algorithm to
find record boundaries. Otherwise, if a `.splitting-bai` index file is found it is used to find
splits. A regular `.bai` index file may optionally be used to find splits, although it does not
protect against regions with very high coverage (oversampling) since it specifies genomic
regions, not file regions.

For writing BAM, it is possible to write `.splitting-bai` indexes at the same time as writing the
BAM file.

For reading VCF, indexes are not used for file splitting.

Writing `.bai`/`.tabix` indexes is not possible at present. These can be generated using existing
tools, such as htsjdk/GATK/ADAM.

### Intervals

For reading BAM and VCF, a range of intervals may be specified to restrict the records that are
loaded. Intervals are specified using htsjdk's `Interval` class.

For reading BAM, when intervals are specified it is also possible to load unplaced unmapped reads if desired.

### Partition Guarantees

For reading query name sorted BAM, paired reads should never be split across partitions. This allows
applications to be sure that a single task will always be able to process read pairs together.

### Stringency

For reading BAM, the stringency settings from htsjdk are supported.

### Testing

All read and write paths are tested on real files from the field (multi-GB in size).
