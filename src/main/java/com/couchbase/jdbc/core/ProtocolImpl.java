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

package com.couchbase.jdbc.core;


import com.couchbase.CBResultSet;
import com.couchbase.ConnectionParameters;
import com.couchbase.jdbc.Protocol;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;

import org.apache.http.impl.client.*;

import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.*;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by davec on 2015-02-22.
 */
public class ProtocolImpl implements Protocol
{
    String url;
    String user;
    String password;
    int connectTimeout=0;
    boolean readOnly = false;


    public String getURL()
    {
        return url;
    }


    public String getUserName()
    {
        return user;
    }


    public String getPassword()
    {
        return password;
    }

    public void setReadOnly( boolean readOnly )
    {
        this.readOnly = readOnly;
    }

    private static final Logger logger = LoggerFactory.getLogger(ProtocolImpl.class);

    CloseableHttpClient httpClient;

    public ProtocolImpl(String url, Properties props)
    {
        this.url = url;
        setConnectionTimeout(props.getProperty(ConnectionParameters.CONNECTION_TIMEOUT));
    }

    public void connect() throws Exception
    {
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(connectTimeout).build();

        httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

        HttpGet httpGet = new HttpGet(url+"/admin/ping");
        httpGet.setHeader("Accept", "application/json");

        try
        {
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

                @Override
                public String handleResponse(
                        final HttpResponse response) throws ClientProtocolException, IOException
                {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };

            String httpResponse = httpClient.execute(httpGet, responseHandler);

        }
        catch(Exception ex)
        {
            logger.error("Error opening connection {}", ex.getMessage());
            throw ex;

        }

    }

    public CBResultSet query(String sql) throws SQLException
    {
        List<NameValuePair> valuePair = new ArrayList<NameValuePair>();
        valuePair.add(new BasicNameValuePair("statement", sql));

        String select = URLEncodedUtils.format(valuePair, "UTF-8");

        HttpGet httpGet = new HttpGet(url + "/query/service?" + select );
        httpGet.setHeader("Accept", "application/json");

        try
        {
            ResponseHandler<String> responseHandler = new ResponseHandler<String>()
            {

                @Override
                public String handleResponse(
                        final HttpResponse response) throws ClientProtocolException, IOException
                {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300)
                    {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    }
                    else
                    {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };

            String httpResponse = httpClient.execute(httpGet, responseHandler);
            JsonReader jsonReader = Json.createReader(new StringReader(httpResponse));
            JsonObject jsonObject = jsonReader.readObject();

            return new CBResultSet(jsonObject);

        }
        catch (Exception ex)
        {
            throw new SQLException("Error querying cluster", ex);
        }

    }

    public int executeUpdate(String query) throws SQLException
    {
        try
        {
            HttpPost httpPost = new HttpPost(url + "/query/service");
            httpPost.setHeader("Accept", "application/json");

            List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();

            nameValuePairs.add(new BasicNameValuePair("statement", query));
            httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs));
            CloseableHttpResponse response = httpClient.execute(httpPost);
            int status = response.getStatusLine().getStatusCode();

            if ( status >= 200 && status < 300 )
            {
                HttpEntity entity = response.getEntity();
                JsonReader jsonReader = Json.createReader(new StringReader(EntityUtils.toString(entity)));

                JsonObject jsonObject = jsonReader.readObject();
                String statusString = jsonObject.getString("status");

                if (statusString.equals("errors"))
                {
                    JsonArray errors= jsonObject.getJsonArray("errors");
                    JsonObject error = errors.getJsonObject(0);
                    throw new SQLException(error.getString("msg"));
                }
                else if (statusString.equals("success"))
                {
                    JsonObject metrics = jsonObject.getJsonObject("metrics");
                    return metrics.getInt("mutationCount");
                }
                else
                {
                    logger.error("Unexpected status string {} for query {}", statusString, query);
                    throw new SQLException("Unexpected status: " + statusString);
                }
            }

            else
            {
                throw new ClientProtocolException("Unexpected response status: " + status);
            }
        }
        catch (Exception ex)
        {
            logger.error ("Error executing update query {} {}", query, ex.getMessage());
            throw new SQLException("Error executing update",ex.getCause());
        }

    }
    public void setConnectionTimeout(String timeout)
    {
        if (timeout!=null)
        {
            connectTimeout=Integer.parseInt(timeout);
        }
    }
    public void setConnectionTimeout(int timeout )
    {
        connectTimeout=timeout;
    }
}
