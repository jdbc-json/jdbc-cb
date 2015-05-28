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

package com.couchbase.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonArray;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by davec on 2015-05-22.
 */
public class Cluster
{
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    List<Instance> endpoints = new ArrayList<Instance>();
    int i=0;
    /*
    [{"cluster":"default","name":"10.168.209.119","queryEndpoint":"http://10.168.209.119:8093/query/service","adminEndpoint":"http://10.168.209.119:8093/admin","options":null},
    {"cluster":"default","name":"10.169.93.182","queryEndpoint":"http://10.169.93.182:8093/query/service","adminEndpoint":"http://10.169.93.182:8093/admin","options":null},
    {"cluster":"default","name":"10.181.71.84","queryEndpoint":"http://10.181.71.84:8093/query/service","adminEndpoint":"http://10.181.71.84:8093/admin","options":null},
    {"cluster":"default","name":"10.30.210.238","queryEndpoint":"http://10.30.210.238:8093/query/service","adminEndpoint":"http://10.30.210.238:8093/admin","options":null}]
     */

    public Cluster( JsonArray jsonArray )
    {
        int size = jsonArray.size();
        for(int i=0; i<size;i++)
        {
            endpoints.add(new Instance(jsonArray.getJsonObject(i)));
        }
    }
    public String getNextEndpoint()
    {
        //return "http://54.237.32.30:8093/query/service";

        if (i++ > endpoints.size()-1) i=0;
        logger.trace( "Endpoint {} of ",i,endpoints.size());
        return endpoints.get(i).getQueryEndPoint();

    }
}
