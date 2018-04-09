package org.seqdoop.hadoop_bam.spark;

import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class DanglingInputStream extends SeekableStream {

  private static Set<DanglingInputStream> all = new LinkedHashSet<>();

  private String st;
  private SeekableStream ss;

  public DanglingInputStream(SeekableStream ss) {
    this.ss = ss;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(bos);
    new Throwable().printStackTrace(pw);
    pw.close();
    this.st = new String(bos.toByteArray());
    all.add(this);
  }

  public static void dump() {
    System.out.println("Dangling input streams");
    for (DanglingInputStream dis : all) {
      System.out.println(dis);
    }
  }

  @Override
  public String toString() {
    return st;
  }

  @Override
  public long length() {
    return ss.length();
  }

  @Override
  public long position() throws IOException {
    return ss.position();
  }

  @Override
  public void seek(long l) throws IOException {
    ss.seek(l);
  }

  @Override
  public int read() throws IOException {
    return ss.read();
  }

  @Override
  public int read(byte[] bytes, int i, int i1) throws IOException {
    return ss.read(bytes, i, i1);
  }

  @Override
  public void close() throws IOException {
    all.remove(this);
    ss.close();
  }

  @Override
  public boolean eof() throws IOException {
    return ss.eof();
  }

  @Override
  public String getSource() {
    return ss.getSource();
  }
}
