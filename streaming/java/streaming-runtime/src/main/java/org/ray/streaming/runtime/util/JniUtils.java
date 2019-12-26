package org.ray.streaming.runtime.util;

import java.lang.reflect.Field;

import com.google.common.base.Strings;
import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.util.FileUtil;
import org.slf4j.Logger;

public class JniUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JniUtils.class);

  public static void loadLibrary(String libraryName) {
    try {
      System.loadLibrary(libraryName);
    } catch (UnsatisfiedLinkError error) {
      LOG.info("Loading native library {}.", libraryName);
      // Load native library.
      String fileName = System.mapLibraryName(libraryName);
      String libPath = null;
      try (FileUtil.TempFile libFile = FileUtil.getTempFileFromResource(fileName)) {
        libPath = libFile.getFile().getAbsolutePath();
        System.load(libPath);
      }
      LOG.info("Native library loaded.");
      resetLibraryPath(libPath);
    }
  }

  /**
   * @see RayNativeRuntime resetLibraryPath
   */
  private static void resetLibraryPath(String libPath) {
    if (Strings.isNullOrEmpty(libPath)) {
      return;
    }
    String path = System.getProperty("java.library.path");
    if (Strings.isNullOrEmpty(path)) {
      path = "";
    } else {
      path += ":";
    }
    path += String.join(":", libPath);

    // This is a hack to reset library path at runtime,
    // see https://stackoverflow.com/questions/15409223/.
    System.setProperty("java.library.path", path);
    // Set sys_paths to null so that java.library.path will be re-evaluated next time it is needed.
    final Field sysPathsField;
    try {
      sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
      sysPathsField.setAccessible(true);
      sysPathsField.set(null, null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      LOG.error("Failed to set library path.", e);
    }
  }
}
