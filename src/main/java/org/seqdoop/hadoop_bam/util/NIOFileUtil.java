package org.seqdoop.hadoop_bam.util;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NIOFileUtil {

  private static final Logger logger = LoggerFactory.getLogger(NIOFileUtil.class);

  private NIOFileUtil() {
  }

  static final String PARTS_GLOB = "glob:**/part-[mr]-[0-9][0-9][0-9][0-9][0-9]*";

  /**
   * Convert the given path {@link URI} to a {@link Path} object.
   * @param uri the path to convert
   * @return a {@link Path} object
   */
  public static Path asPath(URI uri) {
    try {
      return Paths.get(uri);
    } catch (FileSystemNotFoundException e) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) {
        throw e;
      }
      try {
        return FileSystems.newFileSystem(uri, new HashMap<>(), cl).provider().getPath(uri);
      } catch (IOException ex) {
        throw new RuntimeException("Cannot create filesystem for " + uri, ex);
      }
    }
  }

  /**
   * Convert the given path string to a {@link Path} object.
   * @param path the path to convert
   * @return a {@link Path} object
   */
  public static Path asPath(String path) {
    URI uri = URI.create(path);
    return uri.getScheme() == null ? Paths.get(path) : asPath(uri);
  }

  /**
   * Delete the given directory and all of its contents if non-empty.
   * @param directory the directory to delete
   * @throws IOException
   */
  static void deleteRecursive(Path directory) throws IOException {
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }
      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Returns all the files in a directory that match the given pattern, and that don't
   * have the given extension.
   * @param directory the directory to look for files in, subdirectories are not
   *                  considered
   * @param syntaxAndPattern the syntax and pattern to use for matching (see
   * {@link java.nio.file.FileSystem#getPathMatcher}
   * @param excludesExt the extension to exclude, or null to exclude nothing
   * @return a list of files, sorted by name
   * @throws IOException
   */
  static List<Path> getFilesMatching(Path directory,
      String syntaxAndPattern, String excludesExt) throws IOException {
    PathMatcher matcher = directory.getFileSystem().getPathMatcher(syntaxAndPattern);
    List<Path> parts = Files.walk(directory)
        .filter(matcher::matches)
        .filter(path -> excludesExt == null || !path.toString().endsWith(excludesExt))
        .collect(Collectors.toList());
    Collections.sort(parts);
    return parts;
  }

  /**
   * Merge the given part files in order into an output stream.
   * @param parts the part files to merge
   * @param outputPath the output path to write each file into, in order
   * @throws IOException
   */
  static void mergeInto(List<Path> parts, Path outputPath, FileSystem filesystem)
      throws IOException {
    System.out.println("tw: " + parts);
    parts.forEach(p -> System.out.println(p.toUri()));
    try {
      logger.warn("Attempting to use concat to merge files");
      concat(parts, outputPath, filesystem);
    } catch (UnsupportedOperationException e) {
      logger.warn("Concat not supported, merging serially");
      System.out.println("Concat not supported, merging serially");
      try (final OutputStream out = Files.newOutputStream(outputPath)) {
        for (final Path part : parts) {
          Files.copy(part, out);
        }
      }
      for (final Path part : parts) {
        Files.delete(part);
      }
    }
  }

  static void concat(List<Path> parts, Path outputPath, FileSystem filesystem) throws IOException {
    org.apache.hadoop.fs.Path[] fsParts = parts.stream()
        .map(p -> new org.apache.hadoop.fs.Path(p.toUri()))
        .collect(Collectors.toList())
        .toArray(new org.apache.hadoop.fs.Path[parts.size()]);
    org.apache.hadoop.fs.Path target = new org.apache.hadoop.fs.Path(outputPath.toUri());
    filesystem.create(target); // target must already exist for concat
    try {
      filesystem.concat(target, fsParts);
    } catch (UnsupportedOperationException e) {
      filesystem.delete(target, false); // TODO: delete for other exceptions?
    }
  }

  /**
   * Merge the given part files in order into an output stream.
   * @param parts the part files to merge
   * @param out the stream to write each file into, in order
   * @throws IOException
   */
  static void mergeInto(List<Path> parts, OutputStream out)
      throws IOException {
    parts.forEach(p -> System.out.println(p.toUri()));
    for (final Path part : parts) {
      Files.copy(part, out);
    }
    for (final Path part : parts) {
      Files.delete(part);
    }
  }
}
