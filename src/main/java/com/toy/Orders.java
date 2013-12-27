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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.*;

import java.nio.ByteBuffer;

/**
 *
 * Utility class to send / receive orders from Zookeeper
 *
 */
public class Orders
{

    /**
     * Get an order queue instance as a producer
     * @param zookeeper
     * @param zkPath
     * @return
     */
    public static final DistributedQueue<Integer> getDistributedQueue(
            final CuratorFramework zookeeper, final String zkPath)
    {
        return QueueBuilder.builder(zookeeper, null, new IntegerSerializer(), zkPath).buildQueue();
    }

    /**
     * Get an order queue instance as a consumer
     * @param zookeeper
     * @param callback
     * @param zkPath
     * @return
     */
    public static final DistributedQueue<Integer> getQueueAsConsumer(final CuratorFramework zookeeper, TOYMaster callback, String zkPath)
    {
        return QueueBuilder.builder(zookeeper, callback, new IntegerSerializer(), zkPath).buildQueue();
    }

    /**
     * Just transform an int into a byte array
     */
    private static final class IntegerSerializer implements QueueSerializer<Integer>
    {
        @Override
        public byte[] serialize(Integer i)
        {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(i);
            return bb.array();
        }

        @Override
        public Integer deserialize(byte[] bytes)
        {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            return bb.getInt();
        }
    }

}
