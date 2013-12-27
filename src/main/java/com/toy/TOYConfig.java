package com.toy;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TOYConfig
{
    private static final Logger LOG = LoggerFactory.getLogger(TOYConfig.class);

    public enum Command
    {
        START,
        STOP,
        STATUS,
        ADD
    }

    public final String zookeeper;
    public final String tomcat;
    public final String war;
    public final String queue;
    public final String memory;
    public final String ns;
    public final Command command;

    public static final TOYConfig parse(String[] args)
    {
        CommandLineParser parser = new BasicParser();
        CommandLine commandLine = null;
        Command command = null;
        try
        {
            commandLine = parser.parse(get(), args);
        } catch (ParseException e)
        {
            LOG.error("\n{}", e.getLocalizedMessage());
            LOG.debug("Can't parse cmd line",e);
        }
        if (commandLine == null)
        {
            printHelp();
            System.exit(-1);
        }

        // start, stop or status ?
        boolean start = commandLine.hasOption("start");
        boolean stop = commandLine.hasOption("stop");
        boolean status = commandLine.hasOption("status");
        boolean add = commandLine.hasOption("add");

        if (start && stop || stop && status)
        {
            printHelp();
            System.exit(-1);
        }
        if (add) command = Command.ADD;
        if (start) command = Command.START;
        if (stop) command = Command.STOP;
        if (status) command = Command.STATUS;

        if (command == Command.ADD || command == Command.START)
                Preconditions.checkNotNull(commandLine.getOptionValue("war"), "Please specify application with -war");

        return new TOYConfig(commandLine.getOptionValue("zookeeper"),
                               commandLine.getOptionValue("tomcat"),
                               commandLine.getOptionValue("war"),
                               commandLine.getOptionValue("queue", "default"),
                               commandLine.getOptionValue("memory", "64"),
                               commandLine.getOptionValue("ns", "default"),
                               command
                              );

    }

    private static final void printHelp()
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("toy.sh", get(), true);
    }

    private static final Options get()
    {
        Option tomcat = OptionBuilder.withArgName("tomcat")
                                      .isRequired(false)
                                      .hasArg()
                                      .withDescription("Tomcat home")
                                      .create( "tomcat" );

        Option war = OptionBuilder.withArgName("war")
                .isRequired(false)
                .hasArg()
                .withDescription("WAR file")
                .create( "war" );

        Option zookeeper = OptionBuilder.withArgName("zookeeper")
                                        .isRequired()
                                        .hasArg()
                                        .withDescription("zookeeper servers list e.g. : node1,node2,node3")
                                        .create( "zookeeper" );

        Option queue = OptionBuilder.withArgName("queue")
                .hasArg()
                .withDescription("YARN queue (default : default)")
                .create( "queue" );

        Option maxmem = OptionBuilder.withArgName("memory")
                .hasArg()
                .withDescription("Maximum memory allowed by the YARN container for the Tomcat instance (default : 64M)")
                .create( "memory" );

        Option ns = OptionBuilder.withArgName("namespace")
                .hasArg()
                .withDescription("Deployment namespace (default : default)")
                .create( "namespace" );

        Option log4j = OptionBuilder.withArgName("log4j")
                .hasArg()
                .withDescription("Log4j configuration file, will be added to the Tomcat classpath")
                .create( "log4j" );

        OptionGroup optionGroup = new OptionGroup();
        Option start = OptionBuilder.withArgName("start")
                .hasArg(false)
                .withDescription("Start containers with given WAR")
                .create( "start" );
        Option stop = OptionBuilder.withArgName("stop")
                .hasArg()
                .hasArg(false)
                .withDescription("Stop containers with given WAR")
                .create( "stop" );
        Option status = OptionBuilder.withArgName("status")
                .hasArg()
                .hasArg(false)
                .withDescription("Give status about containers")
                .create( "status" );
        Option add = OptionBuilder.withArgName("add")
                .hasArg()
                .hasArg(false)
                .withDescription("Add new container")
                .create( "add" );

        optionGroup.addOption(start).addOption(stop).addOption(status).addOption(add).setRequired(true);


        return (new Options()).addOptionGroup(optionGroup).addOption(tomcat).addOption(zookeeper).addOption(war)
                              .addOption(queue).addOption(maxmem).addOption(ns);
    }

    private TOYConfig(String zookeeper, String tomcat, String war, String queue, String memory, String namespace, Command command)
    {
        this.zookeeper = zookeeper;
        this.tomcat = tomcat;
        this.war = war;
        this.memory = memory;
        this.queue = queue;
        this.ns = (namespace==null) ? Constants.DEFAULT_NAMESPACE : namespace;
        this.command = command;
    }
}
