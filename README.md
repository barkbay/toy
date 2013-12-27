T.O.Y. : Tomcat On YARN
=======================

What is it ?
------------

I wanted to learn myself how YARN works, as an exercise I decided to try to deploy JEE
web application (a.k.a. WAR files) inside YARN containers.

TOY is just a proof of concept, I did it on my spare time, it is definitely not a
reference for implementing YARN applications.
If you want to to do so I suggest you to have a look at [Apache Twill](http://twill.incubator.apache.org/) project.

Requirements
------------

You need :
- Hadoop 2.2.X :)
- Zookeeper 3.X.X : Zookeeper is used to control WAR deployment lifecycle (add / stop containers) and as a service directory
- Tomcat 7.0.X on your **local** workstation, required libraries will be automatically pushed to your Hadoop cluster from here

You need to check that the following env variables are set :
- **HADOOP_CONF_DIR** : your Hadoop configuration directory
- **HADOOP_YARN_HOME** : used to find the bin/yarn script


Build TOY
---------

```
mvn clean assembly:assembly
```

This will create toy-1.0-SNAPSHOT-bin.tar.gz in the target directory.

How to deploy a WAR
-------------------

```
./toy.sh -start -war <path to your war>/examples.war -tomcat <path to local tomcat>/apache-tomcat-7.0.37 -zookeeper <zk host(s)>
```

Retrieve hosts and ports
------------------------

Tomcat is started on random hosts and ports, use the following command to know which one is used :

```
./toy.sh -status -zookeeper <zk hosts>
```

You should see something like that :

```
[namepace=default]
 examples.war
  http://10.0.3.220:56424
  http://10.0.3.47:41520
```

Basically it means that your web application has been deployed on 2 containers and that
you can use your browser at http://10.0.3.220:56424 and http://10.0.3.47:41520

Start additional Tomcat instances
---------------------------------

```
./toy.sh -add -zookeeper <zk hosts> -war <war name>
```

Running the status command you should see a new instance :

```
[namepace=default]
 examples.war
  http://10.0.3.47:54266
  http://10.0.3.220:56424
  http://10.0.3.47:41520
```


