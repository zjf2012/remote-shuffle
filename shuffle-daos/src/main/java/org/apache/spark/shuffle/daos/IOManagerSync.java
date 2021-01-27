/*
 * (C) Copyright 2018-2020 Intel Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of the Apache License as
 * provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 */

package org.apache.spark.shuffle.daos;

import io.daos.obj.DaosObject;
import org.apache.spark.SparkConf;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IOManagerSync extends IOManager {

  private Map<DaosReader, Integer> readerMap = new ConcurrentHashMap<>();

  private Map<DaosWriterSync, Integer> writerMap = new ConcurrentHashMap<>();

  private DaosWriterSync.WriteConfig writeConfig;

  private BoundThreadExecutors readerExes;

  private BoundThreadExecutors writerExes;

  private static final Logger logger = LoggerFactory.getLogger(IOManagerSync.class);

  public IOManagerSync(SparkConf conf, Map<String, DaosObject> objectMap) {
    super(conf, objectMap);
    this.writeConfig = loadWriteConfig(conf);
    this.readerExes = createReaderExes();
    this.writerExes = createWriterExes();
  }

  @Override
  public DaosWriterSync getDaosWriter(int numPartitions, int shuffleId, long mapId) throws IOException {
    long appId = parseAppId(conf.getAppId());
    if (logger.isDebugEnabled()) {
      logger.debug("getting daoswriter for app id: " + appId + ", shuffle id: " + shuffleId + ", map id: " + mapId +
          ", numPartitions: " + numPartitions);
    }
    DaosWriterSync.WriteParam param = new DaosWriterSync.WriteParam();
    param.numPartitions(numPartitions)
        .shuffleId(shuffleId)
        .mapId(mapId)
        .config(writeConfig);
    DaosWriterSync writer = new DaosWriterSync(param, getObject(appId, shuffleId),
        writerExes == null ? null : writerExes.nextExecutor());
    writer.setWriterMap(writerMap);
    return writer;
  }

  @Override
  public DaosReader getDaosReader(int shuffleId) throws IOException {
    return null;
  }

  @Override
  void close() {
    if (readerExes != null) {
      readerExes.stop();
      readerMap.keySet().forEach(r -> r.close());
      readerMap.clear();
      readerExes = null;
    }
    if (writerExes != null) {
      writerExes.stop();
      writerMap.keySet().forEach(r -> r.close());
      writerMap.clear();
      writerExes = null;
    }
  }

  protected static DaosWriterSync.WriteConfig loadWriteConfig(SparkConf conf) {
    DaosWriterSync.WriteConfig config = new DaosWriterSync.WriteConfig();
    config.warnSmallWrite((boolean)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_WARN_SMALL_SIZE()));
    config.bufferSize((int) ((long)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_SINGLE_BUFFER_SIZE())
        * 1024 * 1024));
    config.minSize((int) ((long)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_MINIMUM_SIZE()) * 1024));
    config.timeoutTimes((int)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_WAIT_DATA_TIMEOUT_TIMES()));
    config.waitTimeMs((int)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_WAIT_MS()));
    config.totalInMemSize((long)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_MAX_BYTES_IN_FLIGHT()) * 1024);
    config.totalSubmittedLimit((int)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_SUBMITTED_LIMIT()));
    config.threads((int)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_THREADS()));
    config.fromOtherThreads((boolean)conf
        .get(package$.MODULE$.SHUFFLE_DAOS_WRITE_IN_OTHER_THREAD()));
    logger.info("write configs, " + config);
    return config;
  }

  private BoundThreadExecutors createWriterExes() {
    if (writeConfig.isFromOtherThreads()) {
      BoundThreadExecutors executors;
      int threads = writeConfig.getThreads();
      if (threads == -1) {
        threads = conf.getInt(SparkLauncher.EXECUTOR_CORES, 1);
      }
      executors = new BoundThreadExecutors("write_executors", threads,
          new DaosWriterSync.WriteThreadFactory());
      logger.info("created BoundThreadExecutors with " + threads + " threads for write");
      return executors;
    }
    return null;
  }

  private BoundThreadExecutors createReaderExes() {
    boolean fromOtherThread = (boolean)conf
        .get(package$.MODULE$.SHUFFLE_DAOS_READ_FROM_OTHER_THREAD());
    if (fromOtherThread) {
      BoundThreadExecutors executors;
      int threads = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_THREADS());
      if (threads == -1) {
        threads = conf.getInt(SparkLauncher.EXECUTOR_CORES, 1);
      }
      executors = new BoundThreadExecutors("read_executors", threads,
          new DaosReader.ReadThreadFactory());
      logger.info("created BoundThreadExecutors with " + threads + " threads for read");
      return executors;
    }
    return null;
  }
}
