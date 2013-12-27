package com.toy;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.DistributedQueue;
import org.apache.curator.utils.ZKPaths;

import java.util.List;

/**
 *
 * Utility class that connects to Zookeeper to retrieve informations
 * or request new Tomcat container.
 *
 */
public class InstancesManagement
{

    public static void status(CuratorFramework zookeeper) throws Exception
    {
        List<String> ns = zookeeper.getChildren().forPath(Constants.ZK_PREFIX);
        for(String namespace : ns)
        {
            System.out.println("[namepace=" + namespace + "]");
            final String nsZnode = ZKPaths.makePath(Constants.ZK_PREFIX, namespace);
            final List<String> apps = zookeeper.getChildren()
                    .forPath(nsZnode);
            for(String app : apps)
            {
                System.out.println(" " + app);
                final String appZnode = ZKPaths.makePath(nsZnode, app);
                final List<String> instances =
                        zookeeper.getChildren().forPath(appZnode);
                for (String instance : instances)
                {
                    if (instance.startsWith("slave"))
                    {
                        final String slaveZnode = ZKPaths.makePath(appZnode, instance);
                        final byte[] bytes = zookeeper.getData().forPath(slaveZnode);
                        System.out.println("  " + new String(bytes, Charsets.UTF_8));
                    }
                }
            }
        }
    }

    public static void add(CuratorFramework zookeeper, String ns, String war, int count)
            throws Exception
    {
        if (count > 0)
            System.out.println("Add " + count + " instances");
        else if (count < 0)
            System.out.println("Remove " + count + " instances");
        else
            System.exit(0);

        final DistributedQueue<Integer> queue =
                Orders.getDistributedQueue(zookeeper, Constants.ZK_PREFIX + "/" + ns + "/" + war + Constants.ORDERS);
        queue.start();
        queue.put(count);
    }
}
