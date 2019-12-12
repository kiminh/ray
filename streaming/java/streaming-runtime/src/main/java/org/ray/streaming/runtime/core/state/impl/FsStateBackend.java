package org.ray.streaming.runtime.core.state.impl;

import com.alibaba.hdfs.fs.PanguAbstractFileSystem;
import com.alibaba.hdfs.fs.PanguFileSystem;
import com.alibaba.pangu.exceptions.PanguFileAlreadyExistsException;
import com.alibaba.security.MySqlUserGroupService;
import com.alibaba.security.UserGroupService;
import com.alipay.streaming.runtime.config.global.StateBackendPanguConfig;
import com.alipay.streaming.runtime.config.types.FsBackendCreationType;
import com.alipay.streaming.runtime.state.StateBackend;
import com.alipay.streaming.runtime.utils.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

public class FsStateBackend implements StateBackend<String, byte[], StateBackendPanguConfig> {

  private static final Logger LOG = LoggerFactory.getLogger(FsStateBackend.class);

  private static final int BUFFER_SIZE = 1024 * 1024 * 8;
  private static final int BLOCK_SIZE = 4096;
  private static final short FILE_COPYS = 3;

  private String rootDir;
  private FileSystem fs;

  public FsStateBackend(final StateBackendPanguConfig config) {
    this(config, FsBackendCreationType.CP_FS_BACKEND);
  }

  public FsStateBackend(final StateBackendPanguConfig config,
      FsBackendCreationType fsBackendCreationType) {
    switch (fsBackendCreationType) {
      case CP_FS_BACKEND:
        rootDir = config.panguRootDir();
        break;
      case MASTER_ACTOR_FS_BACKEND:
        rootDir = config.actorFsRootDir();
        break;
      case REST_SERVICE_FS_BACKEND:
        rootDir = config.restServiceFsRootDir();
        break;
      default:
        rootDir = config.panguRootDir();
        break;
    }
    LOG.info("Ready to init fs state backend for {}.", fsBackendCreationType.name());
    init(config);
  }

  @Override
  public void init(final StateBackendPanguConfig config) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Start init fs state backend, config is {}.", config);
    }

    Configuration conf = new Configuration();
    if (!rootDir.startsWith(FileSystem.DEFAULT_FS)) {
      LOG.info("Use hdfs mode.");
      conf.set("fs.defaultFS", config.panguClusterName());
      conf.set("fs.pangu.user.configuration.enable", "false");
      conf.set("fs.pangu.statistic.enable", "false");
      conf.setClass("fs.pangu.impl", PanguFileSystem.class, FileSystem.class);
      conf.setClass("fs.AbstractFileSystem.pangu.impl",
          PanguAbstractFileSystem.class, AbstractFileSystem.class);
      conf.setClass("fs.pangu.usergroup.impl",
          MySqlUserGroupService.class, UserGroupService.class);
      conf.set("fs.pangu.user.mysql.url", config.panguUserMysqlUrl());
    } else {
      LOG.info("Use local mode.");
      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    try {
      fs = FileSystem.get(conf);
    } catch (Throwable e) {
      LOG.error("File system init failed.", e);
      throw new RuntimeException(e);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Finish init fs state backend.");
    }
  }

  @Override
  public boolean exists(String key) throws Exception {
    Path file = new Path(rootDir, key);
    return fs.exists(file);
  }

  public List<String> listFiles() {
    Path path = new Path(rootDir);
    List<String> fileList = new ArrayList<>();
    try {
      for (FileStatus status : fs.listStatus(path)) {
        fileList.add(status.getPath().getName());
      }
    }
    catch (Exception e) {
      LOG.error("path not exist!", e);
    }
    return fileList;
  }

  public void rename(String sourceKey, String dstKey) {
    Path src = new Path(rootDir, sourceKey);
    Path dst = new Path(rootDir, dstKey);
    try {
      LOG.info("renaming {} to {}.", src, dst);
      fs.rename(src, dst);
    }
    catch (Exception e) {
      LOG.error("rename failed.", e);
    }
  }

  @Override
  public byte[] get(final String key) throws Exception {
    if (LOG.isInfoEnabled()) {
      LOG.info("Get value of key {} start.", key);
    }

    try {
      Path file = new Path(rootDir, key);
      if (!fs.exists(file)) {
        LOG.info("State file {} is not exist.", file);
        return null;
      }

      FileStatus status = fs.getFileStatus(file);
      FSDataInputStream in = fs.open(file);

      byte[] readData = new byte[(int)status.getLen()];
      in.readFully(readData);
      in.close();

      if (LOG.isInfoEnabled()) {
        LOG.info("Get value of key {} success.", key);
      }

      return readData;
    } catch (IOException e) {
      LOG.warn("Get value of key {} failed.", key, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<byte[]> batchGet(final List<String> keys) {
    throw new UnsupportedOperationException("batchGet not support.");
  }

  @Override
  public void put(final String key, final byte[] value) throws Exception {
    if (LOG.isInfoEnabled()) {
      LOG.info("Put value of key {} start.", key);
    }

    try {
      Path file = new Path(rootDir, key);
      FSDataOutputStream out;
      try {
        out = fs.create(file, true, BUFFER_SIZE, FILE_COPYS, BLOCK_SIZE);
      } catch (PanguFileAlreadyExistsException e) {
        /*
         In pangu file system, fs.create will firstly check if it's parent dir exits, if not, create it.
         Thus if 2 processes concurrently create a file in the same folder, both of them will
         try to create a same dir, then one of them will throw a PanguFileAlreadyExistsException.
         It's a bug of pangu client. Here we use a tricky way to avoid this issue - create it again.
        */
        out = fs.create(file, true, BUFFER_SIZE, FILE_COPYS, BLOCK_SIZE);
      }
      out.write(value);
      out.flush();
      out.close();

      if (LOG.isInfoEnabled()) {
        LOG.info("Put value of key {} success.", key);
      }
    } catch (IOException e) {
      LOG.warn("Put value of key {} failed.", key, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void batchPut(Map<String, byte[]> batchData) {
    throw new NotImplementedException("batchPut is not implemented!");
  }

  @Override
  public void remove(final String key) throws Exception {
    LOG.info("Remove value of key {} start.", key);
    Path file = new Path(rootDir, key);
    fs.delete(file, true);
    LOG.info("Remove value of key {} success.", key);
  }

  @Override
  public void flush() {

  }
}
