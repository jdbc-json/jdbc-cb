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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by davec on 2015-05-22.
 */
public class Instance
{
    String cluster,
           name,
           queryEndPoint,
           adminEndPoint,
           querySecure,
           adminSecure;

    String options;

    public Instance( Map <String, String> jsonObject )  throws SQLException
    {
        this.cluster        = jsonObject.get("cluster");
        this.name           = jsonObject.get("name");

        this.adminEndPoint  = jsonObject.get("adminEndpoint");
        this.adminSecure    = jsonObject.get("adminSecure");

        isValidURI( adminEndPoint );

        this.queryEndPoint  = jsonObject.get("queryEndpoint");
        this.querySecure    = jsonObject.get("querySecure");

        isValidURI(queryEndPoint);
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
    public String getSecureQueryEndPoint() { return querySecure; }

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

    private boolean isValidURI(String uriStr) throws SQLException
    {
        try
        {
          URI uri = new URI(uriStr);
          return true;
        }
        catch (URISyntaxException e)
        {
            throw new SQLException("Endpoint " + uriStr + " is invalid", e);
        }
    }
    public String getEndpointURL(boolean ssl)
    {
        return ssl?querySecure:queryEndPoint;
    }

    public String toString()
    {
        return name;
    }
}
