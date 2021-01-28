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
import io.netty.buffer.ByteBuf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A abstract class with {@link DaosObject} wrapped to read data from DAOS.
 */
public interface DaosReader {


  DaosObject getObject();

  /**
   * release resources of all {@link BufferSource}
   * bound with this reader.
   */
  void close();

  /**
   * register buffer source for resource cleanup.
   *
   * @param source
   * BufferSource instance
   */
  void register(BufferSource source);

  /**
   * unregister buffer source if <code>source</code> is release already.
   *
   * @param source
   * BufferSource instance
   */
  void unregister(BufferSource source);

  /**
   * set global <code>readMap</code> and hook this reader for releasing resources.
   *
   * @param readerMap
   * global reader map
   */
  void setReaderMap(Map<DaosReader, Integer> readerMap);

  /**
   *
   * @param partSizeMap
   * @param maxBytesInFlight
   * how many bytes can be read concurrently
   * @param maxReqSizeShuffleToMem
   * maximum data can be put in memory
   * @param metrics
   * @return
   */
  BufferSource
      createBufferSource(LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap,
                         long maxBytesInFlight, long maxReqSizeShuffleToMem,
                         ShuffleReadMetricsReporter metrics);

  abstract class BufferSource {
    protected long currentPartSize;

    protected Tuple2<Long, Integer> curMapReduceId;
    protected Tuple2<Long, Integer> lastMapReduceIdForSubmit;
    protected Tuple2<Long, Integer> lastMapReduceIdForReturn;
    protected int curOffset;
    protected boolean newMap;

    protected LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap;
    protected int totalParts;
    protected int partsRead;

    private ShuffleReadMetricsReporter metrics;

    protected BufferSource(LinkedHashMap<Tuple2<Long, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap,
                           ShuffleReadMetricsReporter metrics) {
      this.partSizeMap = partSizeMap;
      this.metrics = metrics;
      totalParts = partSizeMap.size();
    }

    /**
     * get available buffer after iterating current buffer, next buffer in current desc and next desc.
     *
     * @return buffer with data read from DAOS
     * @throws IOException
     */
    public abstract ByteBuf nextBuf() throws IOException;

    public void checkPartitionSize() throws IOException {
      if (lastMapReduceIdForReturn == null) {
        return;
      }
      // partition size is not accurate after compress/decompress
      long size = partSizeMap.get(lastMapReduceIdForReturn)._1();
      if (size < 35 * 1024 * 1024 * 1024 && currentPartSize * 1.1 < size) {
        throw new IOException("expect partition size " + partSizeMap.get(lastMapReduceIdForReturn) +
            ", actual size " + currentPartSize + ", mapId and reduceId: " + lastMapReduceIdForReturn);
      }
      metrics.incRemoteBlocksFetched(1);
    }

    public abstract boolean cleanup(boolean force);

    public void checkTotalPartitions() throws IOException {
      if (partsRead != totalParts) {
        throw new IOException("expect total partitions to be read: " + totalParts + ", actual read: " + partsRead);
      }
    }

    public Tuple2<Long, Integer> lastMapReduceIdForSubmit() {
      return lastMapReduceIdForSubmit;
    }

    public boolean isNewMap() {
      return newMap;
    }

    public void setNewMap(boolean newMap) {
      this.newMap = newMap;
    }
  }

  /**
   * reader configurations, please check configs prefixed with SHUFFLE_DAOS_READ in {@link package$#MODULE$}.
   */
  public static final class ReaderConfig {
    private long minReadSize;
    private long maxBytesInFlight;
    private long maxMem;
    private int readBatchSize;
    private int waitDataTimeMs;
    private int waitTimeoutTimes;
    private boolean fromOtherThread;

    private static final Logger log = LoggerFactory.getLogger(ReaderConfig.class);

    public ReaderConfig() {
      this(true);
    }

    private ReaderConfig(boolean load) {
      if (load) {
        initialize();
      }
    }

    private void initialize() {
      SparkConf conf = SparkEnv.get().conf();
      minReadSize = (long)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_MINIMUM_SIZE()) * 1024;
      this.maxBytesInFlight = -1L;
      this.maxMem = -1L;
      this.readBatchSize = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_BATCH_SIZE());
      this.waitDataTimeMs = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_MS());
      this.waitTimeoutTimes = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_TIMEOUT_TIMES());
      this.fromOtherThread = (boolean)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_FROM_OTHER_THREAD());
      if (log.isDebugEnabled()) {
        log.debug("minReadSize: " + minReadSize);
        log.debug("maxBytesInFlight: " + maxBytesInFlight);
        log.debug("maxMem: " + maxMem);
        log.debug("readBatchSize: " + readBatchSize);
        log.debug("waitDataTimeMs: " + waitDataTimeMs);
        log.debug("waitTimeoutTimes: " + waitTimeoutTimes);
        log.debug("fromOtherThread: " + fromOtherThread);
      }
    }

    public ReaderConfig copy(long maxBytesInFlight, long maxMem) {
      ReaderConfig rc = new ReaderConfig(false);
      rc.maxMem = maxMem;
      rc.minReadSize = minReadSize;
      rc.readBatchSize = readBatchSize;
      rc.waitDataTimeMs = waitDataTimeMs;
      rc.waitTimeoutTimes = waitTimeoutTimes;
      rc.fromOtherThread = fromOtherThread;
      if (maxBytesInFlight < rc.minReadSize) {
        rc.maxBytesInFlight = minReadSize;
      } else {
        rc.maxBytesInFlight = maxBytesInFlight;
      }
      return rc;
    }

    public int getReadBatchSize() {
      return readBatchSize;
    }

    public int getWaitDataTimeMs() {
      return waitDataTimeMs;
    }

    public int getWaitTimeoutTimes() {
      return waitTimeoutTimes;
    }

    public long getMaxBytesInFlight() {
      return maxBytesInFlight;
    }

    public long getMaxMem() {
      return maxMem;
    }

    public long getMinReadSize() {
      return minReadSize;
    }

    public boolean isFromOtherThread() {
      return fromOtherThread;
    }
  }
}
