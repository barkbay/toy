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

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class Client
{

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    /* Hadoop configuration */
    public final Configuration configuration;

    /* YARN application master jar */
    public final String appMasterJar;

    /* TOY configuration */
    final TOYConfig toyConfig;

    /* YARN client */
    private YarnClient yarn;

    /* YARN application ID */
    private ApplicationId appId;

    static Configuration getDefaultConfiguration() throws Exception
    {
        return new YarnConfiguration();
    }

    public static void main(String[] args) throws Exception
    {


        final TOYConfig toyConfig = TOYConfig.parse(args);
        final Client client = new Client(toyConfig, getDefaultConfiguration());
        switch (toyConfig.command)
        {
            case START:
                client.start();
                break;
            case STATUS:
                client.status();
                break;
            case ADD:
                client.add();
                break;
            default:
                throw new NotImplementedException();
        }
    }

    public Client(String appMaster, TOYConfig toyConfig, Configuration configuration)
    {
        this.configuration = configuration;
        this.toyConfig = toyConfig;
        this.appMasterJar = appMaster;
    }

    public Client(TOYConfig toyConfig, Configuration configuration)
    {
        String appmMasterJar = "toy.jar";
        // Attempt to find toy.jar
        String[] files = (new File(".")).list();
        for (String file : files )
        {
            if (file.startsWith("toy") && file.endsWith("jar"))
            {
                appmMasterJar = file;
                LOG.info("Application master jar is {}", appmMasterJar);
                break;
            }
        }
        this.configuration = configuration;
        this.toyConfig = toyConfig;
        this.appMasterJar = appmMasterJar;
    }

    /**
     * Read and display on stdout the content of Zookeeper
     *
     * @throws Exception
     */
    void status() throws Exception
    {
        final CuratorFramework zookeeper =
                CuratorFrameworkFactory.newClient(toyConfig.zookeeper, new RetryOneTime(10000));
        zookeeper.start();
        List<String> ns = null;
        try
        {
            InstancesManagement.status(zookeeper);
        }
        catch (KeeperException.NoNodeException e)
        {
            System.out.println(" * No service registered *");
        }  finally
        {
            zookeeper.close();
        }
    }

    /**
     * Read and display on stdout the content of Zookeeper
     *
     * @throws Exception
     */
    void add() throws Exception
    {
        final CuratorFramework zookeeper =
                CuratorFrameworkFactory.newClient(toyConfig.zookeeper, new RetryOneTime(10000));
        zookeeper.start();
        try
        {
            InstancesManagement.add(zookeeper, toyConfig.ns, toyConfig.war, 1);
        }
        catch (KeeperException.NoNodeException e)
        {
            System.out.println(" * No service registered *");
        }  finally
        {
            zookeeper.close();
        }
    }

    /**
     * Start a new Application Master and deploy the web application on 2 Tomcat containers
     *
     * @throws Exception
     */
    void start() throws Exception
    {

        //Check tomcat dir
        final File tomcatHomeDir = new File(toyConfig.tomcat);
        final File tomcatLibraries = new File(tomcatHomeDir, "lib");
        final File tomcatBinaries = new File(tomcatHomeDir, "bin");
        Preconditions.checkState(tomcatLibraries.isDirectory(), tomcatLibraries.getAbsolutePath() + " does not exist");

        //Check war file
        final File warFile = new File(toyConfig.war);
        Preconditions.checkState(warFile.isFile(), warFile.getAbsolutePath() + " does not exist");

        yarn = YarnClient.createYarnClient();
        yarn.init(configuration);
        yarn.start();

        YarnClientApplication yarnApplication = yarn.createApplication();
        GetNewApplicationResponse newApplication = yarnApplication.getNewApplicationResponse();
        appId = newApplication.getApplicationId();
        ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
        appContext.setApplicationName("Tomcat : " + tomcatHomeDir.getName() + "\n War : " + warFile.getName());
        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // Register required libraries
        Map<String, LocalResource> localResources = new HashMap<>();
        FileSystem fs = FileSystem.get(configuration);
        uploadDepAndRegister(localResources, appId, fs, "lib-ext/curator-client-2.3.0.jar");
        uploadDepAndRegister(localResources, appId, fs, "lib-ext/curator-framework-2.3.0.jar");
        uploadDepAndRegister(localResources, appId, fs, "lib-ext/curator-recipes-2.3.0.jar");

        // Register application master jar
        registerLocalResource(localResources, appId, fs, new Path(appMasterJar));

        // Register the WAR that will be deployed on Tomcat
        registerLocalResource(localResources, appId, fs, new Path(warFile.getAbsolutePath()));

        // Register Tomcat libraries
        for(File lib : tomcatLibraries.listFiles())
        {
            registerLocalResource(localResources, appId, fs, new Path(lib.getAbsolutePath()));
        }

        File juli = new File(tomcatBinaries, "tomcat-juli.jar");
        if (juli.exists())
        {
            registerLocalResource(localResources, appId, fs, new Path(juli.getAbsolutePath()));
        }

        amContainer.setLocalResources(localResources);

        // Setup master environment
        Map<String, String> env = new HashMap<>();
        final String TOMCAT_LIBS = fs.getHomeDirectory() + "/" + Constants.TOY_PREFIX + appId.toString();
        env.put(Constants.TOMCAT_LIBS, TOMCAT_LIBS);

        if (toyConfig.zookeeper != null)
        {
            env.put(Constants.ZOOKEEPER_QUORUM, toyConfig.zookeeper);
        }
        else
        {
            env.put(Constants.ZOOKEEPER_QUORUM, NetUtils.getHostname());
        }

        // 1. Compute classpath
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$())
                .append(File.pathSeparatorChar).append("./*");
        for (String c : configuration.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH))
        {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");

        // add the runtime classpath needed for tests to work
        if (configuration.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false))
        {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }
        env.put("CLASSPATH", classPathEnv.toString());
        env.put(Constants.WAR, warFile.getName());
        // For unit test with YarnMiniCluster
        env.put(YarnConfiguration.RM_SCHEDULER_ADDRESS, configuration.get(YarnConfiguration.RM_SCHEDULER_ADDRESS));
        amContainer.setEnvironment(env);


        // 1.2 Set constraint for the app master
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(32);
        appContext.setResource(capability);


        // 2. Compute app master cmd line
        Vector<CharSequence> vargs = new Vector<>(10);
        // Set java executable command
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx32m");
        // Set class name
        vargs.add(TOYMaster.class.getCanonicalName());
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs)
        {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<>();
        commands.add(command.toString());
        amContainer.setCommands(commands);
        appContext.setAMContainerSpec(amContainer);


        // 3. Setup security tokens
        if (UserGroupInformation.isSecurityEnabled())
        {
            Credentials credentials = new Credentials();
            String tokenRenewer = configuration.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0)
            {
                throw new Exception(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }

            // For now, only getting tokens for the default file-system.
            final org.apache.hadoop.security.token.Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null)
            {
                for (org.apache.hadoop.security.token.Token<?> token : tokens)
                {
                    LOG.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }

        appContext.setQueue("default");
        LOG.info("Submitting TOY application {} to ASM", appId.toString());
        yarn.submitApplication(appContext);

        // Monitor the application
        monitorApplication(appId);
        LOG.info("Client exited");
    }

    private static void registerLocalResource(Map<String, LocalResource> localResources, ApplicationId appId, FileSystem fs, Path src) throws IOException
    {
        String pathSuffix =  Constants.TOY_PREFIX + appId.toString() + "/" + src.getName();
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        LOG.info("Copy {} from local filesystem to {} and add to local environment", src.getName(), dst.toUri());
        fs.copyFromLocalFile(false, true, src, dst);
        FileStatus destStatus = fs.getFileStatus(dst);
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
        amJarRsrc.setType(LocalResourceType.FILE);
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        amJarRsrc.setTimestamp(destStatus.getModificationTime());
        amJarRsrc.setSize(destStatus.getLen());
        localResources.put(src.getName(),  amJarRsrc);
    }

    private void uploadDepAndRegister(Map<String, LocalResource> localResources, ApplicationId appId, FileSystem fs, String depname) throws IOException
    {
        File dep = new File(depname);
        if (!dep.exists()) throw  new IOException(dep.getAbsolutePath() + " does not exist");
        Path dst = new Path(fs.getHomeDirectory(), Constants.TOY_PREFIX + appId.toString() + "/" + dep.getName());
        LOG.info("Copy {} from local filesystem to {} and add to local environment", dep.getName(), dst.toUri());
        FileInputStream input = new FileInputStream(dep);
        final FSDataOutputStream outputStream = fs.create(dst, true);
        ByteStreams.copy(input, outputStream);
        input.close();
        outputStream.close();
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);
        amJarRsrc.setType(LocalResourceType.FILE);
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        FileStatus destStatus = fs.getFileStatus(dst);
        amJarRsrc.setTimestamp(destStatus.getModificationTime());
        amJarRsrc.setSize(destStatus.getLen());
        localResources.put(dep.getName(),  amJarRsrc);

    }

    private boolean monitorApplication(ApplicationId appId) throws Exception
    {

        while (true)
        {

            // Check app status every 10 second.
            try
            {
                Thread.sleep(10000);
            } catch (InterruptedException e)
            {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarn.getApplicationReport(appId);

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state)
            {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus)
                {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else
                {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state)
            {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }
        }
    }
}
