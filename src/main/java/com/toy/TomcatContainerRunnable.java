package com.toy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Invoked by YARN application master to start new TOMCAT containers concurrently
 */
public class TomcatContainerRunnable implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger(TomcatContainerRunnable.class);

    final private Container container;
    final private FileSystem fs;
    final private Configuration conf;
    final private NMClientAsync nmClientAsync;
    final TOYMaster.NMCallbackHandler containerListener;
    final static ContainerId containerId = ConverterUtils.toContainerId(System.getenv()
            .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
    final static String pathSuffix = System.getenv().get(Constants.TOMCAT_LIBS);
    final static String war = System.getenv().get(Constants.WAR);
    final static Path path = new Path(pathSuffix);

    public TomcatContainerRunnable(Container container,
                                   TOYMaster.NMCallbackHandler containerListener,
                                   FileSystem fs,
                                   Configuration conf,
                                   NMClientAsync nmClientAsync)
    {
        this.container = container;
        this.containerListener = containerListener;
        this.fs = fs;
        this.conf = conf;
        this.nmClientAsync = nmClientAsync;
    }

    @Override
    public void run()
    {
        LOG.info("Setting up Tomcat container launch for container id {} / war {}", container.getId(), war);
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        // Set the local resources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        try
        {
            final RemoteIterator<LocatedFileStatus> libs = fs.listFiles(path, false);
            while (libs.hasNext())
            {
                final LocatedFileStatus next = libs.next();
                LOG.debug("Register {} for container", next.getPath());
                LocalResource lib = Records.newRecord(LocalResource.class);
                lib.setType(LocalResourceType.FILE);
                lib.setVisibility(LocalResourceVisibility.APPLICATION);
                lib.setResource(ConverterUtils.getYarnUrlFromURI(next.getPath().toUri()));
                lib.setTimestamp(next.getModificationTime());
                lib.setSize(next.getLen());
                localResources.put(next.getPath().getName(), lib);
            }
            ctx.setLocalResources(localResources);
        } catch (IOException e)
        {
            LOG.error("Error while fetching Tomcat libraries : {}", e.getLocalizedMessage(), e);
        }

        // Build classpath
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$())
                .append(File.pathSeparatorChar).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");

        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }
        Map<String, String> env = new HashMap<String, String>();
        env.put("CLASSPATH", classPathEnv.toString());
        env.put(Constants.WAR, war);
        env.put(Constants.ZOOKEEPER_QUORUM, System.getenv(Constants.ZOOKEEPER_QUORUM));
        ctx.setEnvironment(env);

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);
        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + 32 + "m");
        vargs.add("com.toy.TomcatLauncher");
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Tomcat.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Tomcat.stderr");

        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        ctx.setCommands(commands);

        nmClientAsync.startContainerAsync(container, ctx);
    }
}
