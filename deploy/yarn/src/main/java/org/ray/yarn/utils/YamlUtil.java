package org.ray.yarn.utils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Read YAML file content and convert to objects
 */
public class YamlUtil {
  private static final Log log = LogFactory.getLog(YamlUtil.class);

  public static <T> T loadFile(String filePath, Class<T> clazz) throws IOException {
    throw new NotImplementedException();
  }

  public static <T> T loadFromHttpFile(String filePath, Class<T> clazz) throws IOException {
    throw new NotImplementedException();
  }

  public static <T> T loadFromLocalFile(String filePath, Class<T> clazz) throws IOException {
    throw new NotImplementedException();
  }

  static String getProtocol(String filePath) {
    try {
      URL fileURL = new URI(filePath).toURL();
      return fileURL.getProtocol();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unrecognized path " + filePath);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Unrecognized path " + filePath);
    } catch (IllegalArgumentException e) {
      log.info("Path is not absolute");
      File file = new File(filePath);
      return getProtocol(file);
    }
  }


  static String getProtocol(File file) {
    try {
      URL fileURL = file.toURI().toURL();
      return fileURL.getProtocol();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Unrecognized path " + file.getPath());
    }
  }
}
