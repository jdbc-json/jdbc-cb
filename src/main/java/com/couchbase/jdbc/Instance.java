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

import javax.json.JsonObject;

/**
 * Created by davec on 2015-05-22.
 */
public class Instance
{
    String cluster,
           name,
           queryEndPoint,
           adminEndPoint;
    String options;

    public Instance( JsonObject jsonObject )
    {
        this.cluster        = jsonObject.getString("cluster");
        this.name           = jsonObject.getString("name");

        this.adminEndPoint  = jsonObject.getString("adminEndpoint");
        this.queryEndPoint  = jsonObject.getString("queryEndpoint");
    }
    public String getCluster()
    {
        return cluster;
    }

    public void setCluster(String cluster)
    {
        this.cluster = cluster;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getQueryEndPoint()
    {
        return queryEndPoint;
    }

    public void setQueryEndPoint(String queryEndPoint)
    {
        this.queryEndPoint = queryEndPoint;
    }

    public String getAdminEndPoint()
    {
        return adminEndPoint;
    }

    public void setAdminEndPoint(String adminEndPoint)
    {
        this.adminEndPoint = adminEndPoint;
    }

    public String getOptions()
    {
        return options;
    }

    public void setOptions(String options)
    {
        this.options = options;
    }
}
