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
import com.google.common.io.Files;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;

/**
 *
 * Start a TOMCAT instance, register itself to Zookeeper and deploy the WAR
 *
 */
public class TomcatLauncher implements Closeable
{

    private static final Logger LOG = LoggerFactory.getLogger(TomcatLauncher.class);


    final Tomcat tomcat;

    final File webapps;
    final File work;

    private final String ZK_ROOT = "/toy/default/" + System.getenv(Constants.WAR);
    private final String ZK_MASTER = ZK_ROOT + "/master";

    private final CuratorFramework zookeeper = CuratorFrameworkFactory.newClient(
            System.getenv(Constants.ZOOKEEPER_QUORUM),
            new RetryOneTime(10000)
    );

    public static void main(String[] args) throws Exception
    {
        TomcatLauncher tl = new TomcatLauncher();
        tl.start();
    }

    public TomcatLauncher()
    {
        this(Files.createTempDir());
    }

    public TomcatLauncher(final File tempDir)
    {
        this.webapps = new File("webapps");
        this.work = new File("work");
        this.tomcat = new Tomcat();
    }

    final CountDownLatch latch = new CountDownLatch(1);
    public void start() throws Exception
    {
        zookeeper.start();
        if (!zookeeper.getZookeeperClient().blockUntilConnectedOrTimedOut())
        {
            LOG.error("Unable to connect to Zookeeper @{}", System.getenv(Constants.ZOOKEEPER_QUORUM));
            System.exit(1);
        }
        final Stat masterPathExist = zookeeper.checkExists().usingWatcher(new ZKMasterWatcher()).forPath(ZK_MASTER);
        if (masterPathExist == null)
        {
            throw new Exception("Master znode is not created");
        }

        LOG.info("Tomcat Webapps directory is {}", webapps.getAbsolutePath());
        if (!this.webapps.mkdirs())
            throw new IOException("Impossible to create directory " + this.webapps.getAbsolutePath());
        if (!this.work.mkdirs())
            throw new IOException("Impossible to create directory " + this.webapps.getAbsolutePath());

        tomcat.getHost().setAppBase(this.webapps.getAbsolutePath());

        // Setup a standard HTTP connector
        final Connector connector = new Connector("HTTP/1.1");
        connector.setPort(0);
        tomcat.setConnector(connector);
        tomcat.getService().addConnector(connector);

        final File war = new File(System.getenv(Constants.WAR));
        LOG.info("{} size is {} bytes size", war.getAbsolutePath(), war.length());
        final File warDestination = new File(webapps, war.getName());
        LOG.info("Copy WAR {} to {}", war.getAbsolutePath(), warDestination.getAbsolutePath());
        Files.copy(war, warDestination);
        tomcat.addWebapp("/", warDestination.getAbsolutePath());
        tomcat.start();
        try
        {
        register();
        }
        catch (Exception e)
        {
            LOG.error("Error while registering slave", e);
            latch.countDown();
        }
        latch.await();
        LOG.info("Tomcat instance shutdown in progress");
        tomcat.stop();
    }

    private void register() throws Exception
    {
        zookeeper.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(ZK_ROOT + "/slave-", data());
    }

    private byte[] data() throws Exception
    {
        final String hostname = InetAddress.getLocalHost().getHostAddress();
        final StringBuilder sb = new StringBuilder(128);
        return sb.append("http://").append(hostname).append(':').append(getPort())
                 .toString().getBytes(Charsets.UTF_8);
    }

    public int getPort()
    {
        return tomcat.getConnector().getLocalPort();
    }

    @Override
    public void close() throws IOException
    {
        try
        {
            if (zookeeper != null) zookeeper.close();
        } catch (Exception e)
        {
            LOG.error("Error while shutting down zookeeper client",e);
        }
        try
        {
            tomcat.stop();
        } catch (LifecycleException e)
        {
            LOG.error("Error while stopping Tomcat container", e);
        }
    }

    private class ZKMasterWatcher implements CuratorWatcher
    {
        @Override
        public void process(WatchedEvent watchedEvent) throws Exception
        {
            LOG.info("Tomcat container shutdown triggered from Zookeeper");
            latch.countDown();
        }
    }
}
