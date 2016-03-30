/*
 * //  Copyright (c) 2015 Couchbase, Inc.
 * //  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * //  except in compliance with the License. You may obtain a copy of the License at
 * //    http://www.apache.org/licenses/LICENSE-2.0
 * //  Unless required by applicable law or agreed to in writing, software distributed under the
 * //  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * //  either express or implied. See the License for the specific language governing permissions
 * //  and limitations under the License.
 */

package com.couchbase.jdbc.connect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by davec on 2015-05-22.
 */
public class Cluster
{
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    Integer instanceIndex = new Integer(0);
    AtomicInteger numInstances = new AtomicInteger(0);
    List<Instance> endpoints = new ArrayList<Instance>();

    /*
    [{"cluster":"default","name":"10.168.209.119","queryEndpoint":"http://10.168.209.119:8093/query/service","adminEndpoint":"http://10.168.209.119:8093/admin","options":null},
    {"cluster":"default","name":"10.169.93.182","queryEndpoint":"http://10.169.93.182:8093/query/service","adminEndpoint":"http://10.169.93.182:8093/admin","options":null},
    {"cluster":"default","name":"10.181.71.84","queryEndpoint":"http://10.181.71.84:8093/query/service","adminEndpoint":"http://10.181.71.84:8093/admin","options":null},
    {"cluster":"default","name":"10.30.210.238","queryEndpoint":"http://10.30.210.238:8093/query/service","adminEndpoint":"http://10.30.210.238:8093/admin","options":null}]
     */

    public Cluster( List <Map> jsonArray, boolean ssl )
    {
        int num = jsonArray.size();
        for(int i=0; i < num ;i++)
        {
            try
            {
                //noinspection unchecked
                endpoints.add(new Instance(jsonArray.get(i)));
                numInstances.incrementAndGet();
            }
            catch( SQLException ex)
            {
                logger.debug("Invalid endpoint ", ex.getCause().getMessage());
            }
        }
    }
    public Instance getNextEndpoint()
    {

/*
        Map jsonObject = new HashMap();
        jsonObject.put("queryEndpoint","http://54.237.32.30:8093/query/service" );
        jsonObject.put("adminEndpoint","http://54.237.32.30:8093/query/admin" );

        jsonObject.put("querySecure", "https://54.237.32.30:18093/query/service");
        jsonObject.put("adminSecure", "https://54.237.32.30:18093/query/admin");

        try
        {
            Instance instance = new Instance(jsonObject);
            return instance;
        }
        catch (SQLException ex )
        {
            return null;
        }
*/


        int i;
        synchronized (instanceIndex)
        {
            i = instanceIndex++;

            if (i >= numInstances.get())
            {
                instanceIndex = 0;
                i=0;
            }
        }

        logger.trace( "Endpoint {} of {}",i,numInstances);

        return endpoints.get(i);

    }
    public void addEndPoint(Map endpoint)
    {
        try
        {
            //noinspection unchecked
            endpoints.add(new Instance(endpoint));
            numInstances.incrementAndGet();
        }
        catch( SQLException ex)
        {
            logger.debug("Invalid endpoint ", ex.getCause().getMessage());
        }
    }
    public void invalidateEndpoint(Instance instance)
    {
        logger.trace("Invalidating instance {}", instance);
        synchronized (instanceIndex)
        {
            endpoints.remove(instance);
            numInstances.decrementAndGet();
        }
    }
}
