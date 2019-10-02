/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.replication.regionserver.TestSourceFSConfigurationProvider;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Testcase for HBASE-23098
 */
@Category({ ReplicationTests.class, LargeTests.class })
public class TestNamespaceReplicationWithBulkLoadedData {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNamespaceReplicationWithBulkLoadedData.class);
  private static final Logger LOG =
      LoggerFactory.getLogger(TestNamespaceReplicationWithBulkLoadedData.class);

  protected static final HBaseTestingUtility UTIL1 = new HBaseTestingUtility();
  protected static final HBaseTestingUtility UTIL2 = new HBaseTestingUtility();
  protected static final HBaseTestingUtility UTIL3 = new HBaseTestingUtility();

  protected static final String NS1 = "ns1";
  protected static final String NS2 = "ns2";
  protected static final String NS3 = "ns3";

  protected static final TableName TN1 = TableName.valueOf(NS1 + ":t1_syncup");
  protected static final TableName TN2 = TableName.valueOf(NS2 + ":t2_syncup");
  protected static final TableName TN3 = TableName.valueOf(NS3 + ":t3_syncup");

  protected static final byte[] FAMILY = Bytes.toBytes("cf1");
  protected static final byte[] QUALIFIER = Bytes.toBytes("q1");

  protected static final byte[] NO_REP_FAMILY = Bytes.toBytes("norep");

  protected static TableDescriptor t1SyncupSource;
  protected static TableDescriptor t1SyncupTarget;
  protected static TableDescriptor t2SyncupSource;
  protected static TableDescriptor t2SyncupTarget;
  protected static TableDescriptor t3SyncupSource;
  protected static TableDescriptor t3SyncupTarget;

  protected static Connection conn1;
  protected static Connection conn2;
  protected static Connection conn3;

  protected static Table ht1Source;
  protected static Table ht2Source;
  protected static Table ht3Source;
  protected static Table ht1TargetAtSlave1;
  protected static Table ht2TargetAtSlave1;
  protected static Table ht3TargetAtSlave1;
  protected static Table ht1TargetAtSlave2;
  protected static Table ht2TargetAtSlave2;
  protected static Table ht3TargetAtSlave2;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    customizeClusterConf(UTIL1.getConfiguration());
    customizeClusterConf(UTIL2.getConfiguration());
    customizeClusterConf(UTIL3.getConfiguration());

    TestReplicationBase.configureClusters(UTIL1, UTIL2, UTIL3);
    UTIL1.startMiniZKCluster();
    UTIL2.setZkCluster(UTIL1.getZkCluster());
    UTIL3.setZkCluster(UTIL1.getZkCluster());

    UTIL1.startMiniCluster(2);
    UTIL2.startMiniCluster(4);
    UTIL3.startMiniCluster(3);

    t1SyncupSource = TableDescriptorBuilder.newBuilder(TN1)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
                .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();

    t1SyncupTarget = TableDescriptorBuilder.newBuilder(TN1)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();

    t2SyncupSource = TableDescriptorBuilder.newBuilder(TN2)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
                .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();

    t2SyncupTarget = TableDescriptorBuilder.newBuilder(TN2)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();

    t3SyncupSource = TableDescriptorBuilder.newBuilder(TN3)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
                .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();

    t3SyncupTarget = TableDescriptorBuilder.newBuilder(TN3)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(NO_REP_FAMILY)).build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ht1Source, true);
    Closeables.close(ht2Source, true);
    Closeables.close(ht3Source, true);
    Closeables.close(ht1TargetAtSlave1, true);
    Closeables.close(ht2TargetAtSlave1, true);
    Closeables.close(ht3TargetAtSlave1, true);
    Closeables.close(ht1TargetAtSlave2, true);
    Closeables.close(ht2TargetAtSlave2, true);
    Closeables.close(ht3TargetAtSlave2, true);
    Closeables.close(conn1, true);
    Closeables.close(conn2, true);
    Closeables.close(conn3, true);
    UTIL3.shutdownMiniCluster();
    UTIL2.shutdownMiniCluster();
    UTIL1.shutdownMiniCluster();
  }

  protected static void customizeClusterConf(Configuration conf) {
    conf.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    conf.set(HConstants.REPLICATION_CLUSTER_ID, "12345");
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    conf.set("hbase.replication.source.fs.conf.provider",
        TestSourceFSConfigurationProvider.class.getCanonicalName());
  }

  protected final void setupReplication() throws Exception {
    Admin admin1 = UTIL1.getAdmin();
    admin1.createNamespace(NamespaceDescriptor.create(NS1).build());
    admin1.createNamespace(NamespaceDescriptor.create(NS2).build());
    admin1.createNamespace(NamespaceDescriptor.create(NS3).build());
    admin1.createTable(t1SyncupSource);
    admin1.createTable(t2SyncupSource);
    admin1.createTable(t3SyncupSource);

    Admin admin2 = UTIL2.getAdmin();
    admin2.createNamespace(NamespaceDescriptor.create(NS1).build());
    admin2.createNamespace(NamespaceDescriptor.create(NS2).build());
    admin2.createNamespace(NamespaceDescriptor.create(NS3).build());
    admin2.createTable(t1SyncupTarget);
    admin2.createTable(t2SyncupTarget);
    admin2.createTable(t3SyncupTarget);

    Admin admin3 = UTIL3.getAdmin();
    admin3.createNamespace(NamespaceDescriptor.create(NS1).build());
    admin3.createNamespace(NamespaceDescriptor.create(NS2).build());
    admin3.createNamespace(NamespaceDescriptor.create(NS3).build());
    admin3.createTable(t1SyncupTarget);
    admin3.createTable(t2SyncupTarget);
    admin3.createTable(t3SyncupTarget);

    // Get HTable from Master
    conn1 = ConnectionFactory.createConnection(UTIL1.getConfiguration());
    ht1Source = conn1.getTable(TN1);
    ht2Source = conn1.getTable(TN2);
    ht3Source = conn1.getTable(TN3);

    // Get HTable from slave1
    conn2 = ConnectionFactory.createConnection(UTIL2.getConfiguration());
    ht1TargetAtSlave1 = conn2.getTable(TN1);
    ht2TargetAtSlave1 = conn2.getTable(TN2);
    ht3TargetAtSlave1 = conn2.getTable(TN3);

    // Get HTable from slave2
    conn3 = ConnectionFactory.createConnection(UTIL3.getConfiguration());
    ht1TargetAtSlave2 = conn3.getTable(TN1);
    ht2TargetAtSlave2 = conn3.getTable(TN2);
    ht3TargetAtSlave2 = conn3.getTable(TN3);


    /** peer1 set M-S: Master: all table  Slave1: all table  **/
    ReplicationPeerConfig rpc =
        ReplicationPeerConfig.newBuilder().setClusterKey(UTIL2.getClusterKey()).build();
    admin1.addReplicationPeer("1", rpc);

    /** peer2 set M-S: Master: ns2:t2_syncup Slave2: ns2:t2_syncup **/
    Map<TableName, List<String>> tableCFsMap = new HashMap<>();
    tableCFsMap.put(TN2, null);
    ReplicationPeerConfig rpc2 =
        ReplicationPeerConfig.newBuilder().setClusterKey(UTIL3.getClusterKey())
            .setReplicateAllUserTables(false).setTableCFsMap(tableCFsMap).build();
    admin1.addReplicationPeer("2", rpc2);

    /** peer3 set M-S: Master: ns3 Slave2: ns3 **/
    Set<String> namespaces = new HashSet<>();
    namespaces.add(NS3);
    ReplicationPeerConfig rpc3 =
        ReplicationPeerConfig.newBuilder().setClusterKey(UTIL3.getClusterKey())
            .setReplicateAllUserTables(false).setNamespaces(namespaces).build();
    admin1.addReplicationPeer("3", rpc3);

  }

  @Test
  public void testSyncUpTool() throws Exception {
    /**
     * Set up Replication, The Master has three peers:
     * Peer1:  Master -> Slave1
     * Peer2:  Master:ns2:t2_syncup -> Slave2:ns2:t2_syncup
     * Peer3:  Master:ns3 -> Slave2:ns3
     */
    setupReplication();

    // 1. bulk load files to ht1Source, slave1 is 100, slave2 is 0
    loadAndReplicateHFiles(ht1Source, 100);
    validateHFileReplication(ht1Source, ht1TargetAtSlave1, ht1TargetAtSlave2, 100, 0);

    // 2. bulk load files to ht2Source, slave1 is 200, slave2 is 200
    loadAndReplicateHFiles(ht2Source, 200);
    validateHFileReplication(ht2Source, ht2TargetAtSlave1, ht2TargetAtSlave2, 200, 200);

    // 3. bulk load files to ht3Source, slave1 is 106, slave2 is 106
    loadAndReplicateHFiles(ht3Source, 106);
    validateHFileReplication(ht3Source, ht3TargetAtSlave1, ht3TargetAtSlave2, 106, 106);

    // 4. validate Master hfile-refs is empty
    MiniZooKeeperCluster zkCluster = UTIL1.getZkCluster();
    ZKWatcher watcher = new ZKWatcher(UTIL1.getConfiguration(), "TestZnodeHFiles-refs", null);
    RecoverableZooKeeper zk = ZKUtil.connect(UTIL1.getConfiguration(), watcher);
    ZKReplicationQueueStorage replicationQueueStorage =
        new ZKReplicationQueueStorage(watcher, UTIL1.getConfiguration());
    Set<String> hfiles = replicationQueueStorage.getAllHFileRefs();
    assertTrue(hfiles.isEmpty());
  }

  private List<String> getHFileRange(int range) {
    Set<String> randomHFileRanges = new HashSet<>(28);
    for (int i = 0; i < 28; i++) {
      randomHFileRanges.add(UTIL1.getRandomUUID().toString());
    }
    List<String> randomHFileRangeList = new ArrayList<>(randomHFileRanges);
    Collections.sort(randomHFileRangeList);
    return randomHFileRangeList;
  }

  private void loadAndReplicateHFiles(Table table, int numOfRows) throws Exception {
    LOG.debug("loadAndReplicateHFiles");

    Iterator<String> randomHFileRangeListIterator = getHFileRange(4).iterator();

    int part = numOfRows / 2;
    // Load 50 + 50  hfiles to tableName.
    byte[][][] hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
            Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadAndValidateHFileReplication("HFileReplication_1",
        TestReplicationBase.row, FAMILY, table, hfileRanges, part);

    hfileRanges =
        new byte[][][] { new byte[][] { Bytes.toBytes(randomHFileRangeListIterator.next()),
            Bytes.toBytes(randomHFileRangeListIterator.next()) } };
    loadFromOtherHDFSAndValidateHFileReplication("HFileReplication_1",
        TestReplicationBase.row, FAMILY, table, hfileRanges, part);
  }

  private void validateHFileReplication(Table source,
                                        Table targetAtFirst,
                                        Table targetAtSecond,
                                        int numOfRowsAtFirst,
                                        int numOfRowsAtSecond)
      throws IOException, InterruptedException {
    // ensure replication completed
    int result = HBaseTestingUtility.countRows(targetAtFirst);
    wait(targetAtFirst, numOfRowsAtFirst, source.getName().getNameAsString()
        + " has " + numOfRowsAtFirst +  " rows on source, and " + result + " on first peer.");

    result = HBaseTestingUtility.countRows(targetAtSecond);
    wait(targetAtSecond, numOfRowsAtSecond, source.getName().getNameAsString()
        + " has " + numOfRowsAtSecond +  " rows on source, and " + result + " on second peer.");
  }


  private void loadAndValidateHFileReplication(String testName, byte[] row, byte[] fam,
      Table source, byte[][][] hfileRanges, int numOfRows) throws Exception {
    Path dir = UTIL1.getDataTestDirOnTestFS(testName);
    FileSystem fs = UTIL1.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(fam));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      HFileTestUtil.createHFile(UTIL1.getConfiguration(), fs,
        new Path(familyDir, "hfile_" + hfileIdx++), fam, row, from, to, numOfRows);
    }

    final TableName tableName = source.getName();
    BulkLoadHFiles loader = BulkLoadHFiles.create(UTIL1.getConfiguration());
    loader.bulkLoad(tableName, dir);
  }

  private void loadFromOtherHDFSAndValidateHFileReplication(String testName, byte[] row, byte[] fam,
      Table source, byte[][][] hfileRanges, int numOfRows) throws Exception {
    Path dir = UTIL2.getDataTestDirOnTestFS(testName);
    FileSystem fs = UTIL2.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(fam));

    int hfileIdx = 0;
    for (byte[][] range : hfileRanges) {
      byte[] from = range[0];
      byte[] to = range[1];
      HFileTestUtil.createHFile(UTIL2.getConfiguration(), fs,
          new Path(familyDir, "hfile_" + hfileIdx++), fam, row, from, to, numOfRows);
    }

    final TableName tableName = source.getName();
    BulkLoadHFiles loader = BulkLoadHFiles.create(UTIL1.getConfiguration());
    loader.bulkLoad(tableName, dir);
  }

  private void wait(Table target, int expectedCount, String msg)
      throws IOException, InterruptedException {
    for (int i = 0; i < TestReplicationBase.NB_RETRIES; i++) {
      int rowCountHt2TargetAtPeer1 = HBaseTestingUtility.countRows(target);
      if (i == TestReplicationBase.NB_RETRIES - 1) {
        assertEquals(msg, expectedCount, rowCountHt2TargetAtPeer1);
      }
      if (expectedCount == rowCountHt2TargetAtPeer1) {
        break;
      }
      Thread.sleep(TestReplicationBase.SLEEP_TIME);
    }
  }
}
