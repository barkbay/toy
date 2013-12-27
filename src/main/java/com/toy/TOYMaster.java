/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.toy;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the YARN application master.
 *
 * It will start 2 TOMCAT containers on the cluster and wait for
 * additional orders from Zookeeper.
 *
 */
public class TOYMaster implements QueueConsumer<Integer>
{
    private static final Logger LOG = LoggerFactory.getLogger(TOYMaster.class);

    // Configuration
    private final Configuration configuration;

    private final FileSystem fs;

    private final NMCallbackHandler nodeManagerListener = new NMCallbackHandler();

    private final CuratorFramework zookeeper = CuratorFrameworkFactory.newClient(
            System.getenv(Constants.ZOOKEEPER_QUORUM),
            new RetryOneTime(10000)
    );

    private final String ns = System.getenv(Constants.NAMESPACE);
    private final String ZK_ROOT = (ns==null) ? (Constants.ZK_PREFIX + "/default/" + System.getenv(Constants.WAR))
                                              : (Constants.ZK_PREFIX + "/" + ns + "/" + System.getenv(Constants.WAR));
    private final String ZK_MASTER = ZK_ROOT + "/master";
    private final String ZK_ORDERS = ZK_ROOT + Constants.ORDERS;

    private final AtomicInteger requestedContainers = new AtomicInteger(2);

    ApplicationAttemptId appAttemptID;

    // Handle to communicate with the Resource Manager
    private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;

    private DistributedQueue<Integer> ordersQueue = null;

    public static void main(String[] args) throws Exception
    {
        (new TOYMaster()).run();
    }

    public TOYMaster() throws Exception
    {
        this.configuration = new YarnConfiguration();
        final String rm_address = System.getenv(YarnConfiguration.RM_SCHEDULER_ADDRESS);
        LOG.info("RM is @ {}", rm_address);
        configuration.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, rm_address);
        this.fs = FileSystem.get(configuration);
        init();
    }

    final CountDownLatch latch = new CountDownLatch(1);

    void run() throws Exception
    {
        LOG.info("============== TOYMaster.run() =================");
        final AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(configuration);
        amRMClient.start();

        nmClientAsync = new NMClientAsyncImpl(nodeManagerListener);
        nmClientAsync.init(configuration);
        nmClientAsync.start();

        // Register self with ResourceManager this will start heartbeating to the RM
        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(NetUtils.getHostname(), -1, "");
        LOG.info("============== TOYMaster registered ! =================");

        for (int i = 0; i < requestedContainers.get(); i++)
        {
            amRMClient.addContainerRequest(newTomcatContainer());
        }

        // Ok, just wait for the shutdown order...
        latch.await();

        // Shutdown order received, stop Zookeeper queue consumer
        if (ordersQueue != null) ordersQueue.close();
        // Close Zookeeper connection
        try
        {
            zookeeper.close();
        } catch (Exception e)
        {
            LOG.error("Error while closing Zookeeper connection", e);
        }

        // Let say that everyting is ok
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "All done", null);

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // bye
        LOG.info("============== TOYMaster BYE ! =================");
        amRMClient.close();
    }

    void clean()
    {
        // TODO : clean HDFS !
        Path pathSuffix = new Path(System.getenv().get(Constants.TOMCAT_LIBS));
        try
        {
            final RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(pathSuffix, false);
            while (locatedFileStatusRemoteIterator.hasNext())
            {
                final LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
                LOG.info("Remove {}", next.getPath().toString());
                fs.delete(pathSuffix, false);
            }
        } catch (IOException e)
        {
            LOG.error("Error while cleaning HDFS directory {}", pathSuffix, e);
        }
    }

    void init() throws Exception
    {
        if (System.getenv().containsKey(ApplicationConstants.Environment.CONTAINER_ID.name()))
        {
            ContainerId containerId = ConverterUtils.toContainerId(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }
        Preconditions.checkNotNull(appAttemptID, "No attempt id");
        zookeeper.start();
        if (!zookeeper.getZookeeperClient().blockUntilConnectedOrTimedOut())
        {
            LOG.error("Unable to connect to Zookeeper @{}", System.getenv(Constants.ZOOKEEPER_QUORUM));
            System.exit(1);
        }
        // Create ROOT znode
        try
        {
            zookeeper.create().creatingParentsIfNeeded().forPath(ZK_ROOT);
        }
        catch (KeeperException.NodeExistsException e)
        {
            //Ignore
        }
        // Check if there is no other master alive
        if ( zookeeper.checkExists().forPath(ZK_MASTER) != null )
        {
            throw new Exception("Master is already alive");
        }
        // Create master znode
        final String masterPath = zookeeper.create().withMode(CreateMode.EPHEMERAL).forPath(ZK_MASTER, getMasterInfo().getBytes(Charsets.UTF_8));
        final Stat masterPathExist = zookeeper.checkExists().usingWatcher(new ZKMasterWatcher()).forPath(masterPath);
        if (masterPathExist == null)
        {
            throw new Exception("Master znode is not created");
        }
        // Start order queue
        ordersQueue = Orders.getQueueAsConsumer(zookeeper, this, ZK_ORDERS);
        ordersQueue.start();
    }

    /**
     * Process events from the resource manager
     */
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler
    {

        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses)
        {
            LOG.info("{} Tomcat containers are completed", statuses.size());
            for (ContainerStatus containerStatus : statuses)
            {
                LOG.info("TOY container completed id="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());
            }
        }

        @Override
        public void onContainersAllocated(List<Container> containers)
        {
            LOG.info("Got response for TOY container allocation, allocatedCnt=" + containers.size());
            for (Container allocatedContainer : containers)
            {
                if (requestedContainers.get() > 0)
                {
                    LOG.info("Starting Tomcat in a new container."
                            + ", containerId=" + allocatedContainer.getId()
                            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                            + ":" + allocatedContainer.getNodeId().getPort()
                            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                            + ", containerResourceMemory"
                            + allocatedContainer.getResource().getMemory());

                    TomcatContainerRunnable runnableLaunchContainer =
                            new TomcatContainerRunnable(allocatedContainer, nodeManagerListener, fs, configuration, nmClientAsync);
                    Thread launchThread = new Thread(runnableLaunchContainer);
                    launchThread.start();
                    requestedContainers.decrementAndGet();
                }
                else
                {
                    LOG.info("Ignore container {} from node {}", allocatedContainer.getId(), allocatedContainer.getNodeId());
                    amRMClient.releaseAssignedContainer(allocatedContainer.getId());
                }
            }
        }

        @Override
        public void onShutdownRequest()
        {
            LOG.info("TOY shutdown requested");
            latch.countDown();
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes)
        {
            LOG.info("onNodesUpdated :");
            for(NodeReport report : updatedNodes)
            {
                LOG.info("[REPORT] {}", report.toString());
            }
        }

        @Override
        public float getProgress()
        {
            return 0;
        }

        @Override
        public void onError(Throwable e)
        {
            LOG.error("TOY Master App error : {}", e.getLocalizedMessage(), e);
            latch.countDown();
        }
    }

    static class NMCallbackHandler implements NMClientAsync.CallbackHandler
    {

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse)
        {
            LOG.info("Container {} STARTED", containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus)
        {
            LOG.error("onContainerStatusReceived({},{})", containerId, containerStatus.toString());
        }

        @Override
        public void onContainerStopped(ContainerId containerId)
        {
            LOG.info("Container {} STOPPED", containerId);
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t)
        {
            LOG.error("Container {} FAILED to start with error : {}", containerId, t.getLocalizedMessage(), t);
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t)
        {
            LOG.error("onGetContainerStatusError({},{})", containerId, t.getLocalizedMessage());
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t)
        {
            LOG.error("onStopContainerError({},{})", containerId, t.getLocalizedMessage());
        }
    }

    @Override
    public void consumeMessage(Integer message) throws Exception
    {
        if (message < 0)
        {
            LOG.error("Sorry, remove container is not implemented !");
        }
        else
        {
            LOG.info("Add {} Tomcat containers", message);
            requestedContainers.addAndGet(message);
            for (int i=0; i < message; i++)
            {
                LOG.info("Add a new Tomcat container request");
                amRMClient.addContainerRequest(newTomcatContainer());
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState)
    {
        LOG.info("Zookeeper state changed : {}", newState.toString());
    }

    private class ZKMasterWatcher implements CuratorWatcher
    {
        @Override
        public void process(WatchedEvent watchedEvent) throws Exception
        {
            LOG.info("Shutdown triggered from Zookeeper");
            latch.countDown();
        }
    }

    private AMRMClient.ContainerRequest newTomcatContainer()
    {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(32);
        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, pri);
        LOG.info("Requested Tomcat container: " + request.toString());
        return request;
    }

    private String getMasterInfo() {
        final StringBuilder sb = new StringBuilder(128);
        return sb.append("hostname=").append(NetUtils.getHostname())
                 .append("\nappAttemptID=").append(appAttemptID.toString())
                 .toString();
    }

}
