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
import com.couchbase.jdbc.Cluster;
import com.couchbase.jdbc.Protocol;
import org.apache.http.HttpEntity;
import org.apache.http.HttpVersion;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by davec on 2015-02-22.
 */
public class ProtocolImpl implements Protocol
{


    static final int N1QL_ERROR = -1;
    static final int N1QL_SUCCESS = 0;
    static final int N1QL_RUNNING = 1;
    static final int N1QL_COMPLETED = 2;
    static final int N1QL_STOPPED = 3;
    static final int N1QL_TIMEOUT = 4;
    static final int N1QL_FATAL = 5;


    static final Map <String,Integer> statusStrings = new HashMap<String,Integer>();
    static {
        statusStrings.put( "errors", N1QL_ERROR );
        statusStrings.put( "success", N1QL_SUCCESS );
        statusStrings.put( "running", N1QL_RUNNING );
        statusStrings.put( "completed", N1QL_COMPLETED );
        statusStrings.put( "stopped", N1QL_STOPPED );
        statusStrings.put( "timeout", N1QL_TIMEOUT );
        statusStrings.put( "fatal", N1QL_FATAL );
    }

    String url;
    String user;
    String password;
    String credentials;

    Cluster cluster;

    int connectTimeout=0;
    int queryTimeout=0;
    boolean readOnly = false;
    int updateCount;
    CBResultSet resultSet;
    List <String> batchStatements = new ArrayList<String>();

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

    public String getCredentials() { return credentials; }

    public void setReadOnly( boolean readOnly )
    {
        this.readOnly = readOnly;
    }

    private static final Logger logger = LoggerFactory.getLogger(ProtocolImpl.class);

    CloseableHttpClient httpClient;

    public ProtocolImpl(String url, Properties props)
    {
        if ( props.containsKey("user"))
        {
            user=props.getProperty("user");
        }
        if (props.containsKey("password"))
        {
            password=props.getProperty("password");
        }
        if (props.containsKey("credentials"))
        {
            credentials = props.getProperty("credentials");
        }
        this.url = url;
        setConnectionTimeout(props.getProperty(ConnectionParameters.CONNECTION_TIMEOUT));
    }

    public void connect() throws Exception
    {
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(connectTimeout).build();


        httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

        HttpGet httpGet = new HttpGet(url+"/admin/clusters/default/nodes");
        httpGet.setHeader("Accept", "application/json");

        try
        {
            CloseableHttpResponse httpResponse = httpClient.execute(httpGet);
            cluster  = handleClusterResponse( httpResponse );
        }
        catch(Exception ex)
        {
            logger.error("Error opening connection {}", ex.getMessage());
            throw ex;
        }

    }
    public Cluster handleClusterResponse(CloseableHttpResponse response) throws IOException
    {
        int status = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        String string = EntityUtils.toString(entity);
        logger.trace ("Cluster response {}", string);
        JsonReader jsonReader = Json.createReader(new StringReader(string));

        JsonArray jsonArray = jsonReader.readArray();
        String message="";

        switch (status)
        {
            case 200:
                return new Cluster(jsonArray);
            case 400:
                message = "Bad Request";
                break;
            case 401:
                message = "Unauthorized Request credentials are missing or invalid";
                break;
            case 403:
                message = "Forbidden Request: read only violation or client unauthorized to modify";
                break;
            case 404:
                message = "Not found: Request references an invalid keyspace or there is no primary key";
                break;
            case 405:
                message = "Method not allowed: The REST method type in request is supported";
                break;
            case 409:
                message = "Conflict: attempt to create a keyspace or index that already exists";
                break;
            case 410:
                message = "Gone: The server is doing a graceful shutdown";
                break;
            case 500:
                message = "Internal server error: unforeseen problem processing the request";
                break;
            case 503:
                message = "Service Unavailable: there is an issue preventing the request from being serv serviced";
                break;
        }
        throw new ClientProtocolException(message +": " + status);

    }

    public CBResultSet query(String sql) throws SQLException
    {
        List<NameValuePair> valuePair = new ArrayList<NameValuePair>();
        valuePair.add(new BasicNameValuePair("statement", sql));

        if ( queryTimeout != 0 )
        {
            valuePair.add(new BasicNameValuePair("timeout", ""+queryTimeout+'s'));
        }
        if (credentials != null)
        {
            valuePair.add(new BasicNameValuePair("creds",credentials));
        }

        String select=null;

        try{
            select = new UrlEncodedFormEntity(valuePair,"UTF-8").getContent().toString();
        }

        catch ( Exception ex )
        {

        }

        String endpoint = cluster.getNextEndpoint();
        logger.trace("Using endpoint {}", endpoint);

        HttpGet httpGet = new HttpGet(cluster.getNextEndpoint() +'?' + select );

        httpGet.setHeader("Accept", "application/json");
        logger.trace("Get request {}",httpGet.toString());



        try
        {

            CloseableHttpResponse response = httpClient.execute(httpGet);
            return new CBResultSet(handleResponse(sql, response));

        }
        catch (IOException ex)
        {
            logger.error ("Error executing query [{}] {}", sql, ex.getMessage());
            throw new SQLException("Error executing update",ex.getCause());
        }
    }

    public int executeUpdate(String query) throws SQLException
    {
        boolean hasResultSet = execute(query);
        if (!hasResultSet)
        {
            return getUpdateCount();
        }
        else
        {
            return 0;
        }

    }

    public JsonObject handleResponse(String sql, CloseableHttpResponse response) throws SQLException,IOException
    {
        int status = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();

        String strResponse = EntityUtils.toString(entity);
        logger.trace( "Response to query {} {}", sql, strResponse );

        JsonReader jsonReader = Json.createReader(new StringReader(strResponse));

        JsonObject jsonObject = jsonReader.readObject();
        logger.trace( "response from query {} {}", sql, jsonObject.toString());

        String statusString = jsonObject.getString("status");

        Integer iStatus = statusStrings.get(statusString);
        String message;

        switch (status)
        {
            case 200:
                switch (iStatus.intValue())
                {
                    case N1QL_ERROR:
                        JsonArray errors= jsonObject.getJsonArray("errors");
                        JsonObject error = errors.getJsonObject(0);
                        throw new SQLException(error.getString("msg"));

                    case N1QL_SUCCESS:
                        return jsonObject;

                    case N1QL_COMPLETED:
                    case N1QL_FATAL:
                    case N1QL_RUNNING:
                    case N1QL_STOPPED:
                    case N1QL_TIMEOUT:
                        throw  new SQLException("Invalid status " + statusString );

                    default:
                        logger.error("Unexpected status string {} for query {}", statusString, sql);
                        throw new SQLException("Unexpected status: " + statusString);

                }
            case 400:
                message = "Bad Request";
                break;
            case 401:
                message = "Unauthorized Request credentials are missing or invalid";
                break;
            case 403:
                message = "Forbidden Request: read only violation or client unauthorized to modify";
                break;
            case 404:
                message = "Not found: Request references an invalid keyspace or there is no primary key";
                break;
            case 405:
                message = "Method not allowed: The REST method type in request is supported";
                break;
            case 409:
                message = "Conflict: attempt to create a keyspace or index that already exists";
                break;
            case 410:
                message = "Gone: The server is doing a graceful shutdown";
                break;
            case 500:
                message = "Internal server error: unforeseen problem processing the request";
                break;
            case 503:
                message = "Service Unavailable: there is an issue preventing the request from being serviced";
                logger.debug("Error with the request {}", message);

                JsonObject  errors =  null,
                        warnings = null;

                if (jsonObject.containsKey("errors"))
                {
                    errors= jsonObject.getJsonArray("errors").getJsonObject(0);
                    logger.error("Error Code: {} Message: {} for query {} ",errors.getInt("code"),errors.getString("msg"), sql);
                }
                if ( jsonObject.containsKey("warnings"))
                {
                    warnings = jsonObject.getJsonArray("warnings").getJsonObject(0);
                    logger.error("Warning Code: {} Message: {} for query {}",warnings.getInt("code"), warnings.getString("msg"), sql);
                }


                throw new SQLException(errors.getString("msg") + " query " + sql );

            default:
                throw new ClientProtocolException("Unexpected response status: " + status);

        }
        logger.debug("Error with the request {}", message);
        throw new ClientProtocolException(message +": " + status);
    }

    private static NameValuePair scanConstistency= new BasicNameValuePair("scan_consistency","request_plus");

    public JsonObject doQuery(String query, List <NameValuePair> nameValuePairs ) throws SQLException
    {
        try
        {

            HttpPost httpPost = new HttpPost(cluster.getNextEndpoint());
            httpPost.setHeader("Accept", "application/json");

            logger.trace("do query {}",httpPost.toString());

            nameValuePairs.add(scanConstistency);
            if (credentials != null)
            {
                nameValuePairs.add(new BasicNameValuePair("creds",credentials));
            }

            httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs,"UTF-8"));

            CloseableHttpResponse response = httpClient.execute(httpPost);

            return handleResponse(query, response);

        }
        catch (Exception ex)
        {
            logger.error ("Error executing query [{}] {}", query, ex.getMessage());
            throw new SQLException("Error executing update",ex.getCause());
        }
    }

    public boolean execute(String query) throws SQLException
    {
        try
        {
            List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();

            nameValuePairs.add(new BasicNameValuePair("statement", query));
            if ( queryTimeout != 0 )
            {
                nameValuePairs.add(new BasicNameValuePair("timeout", ""+queryTimeout+'s'));
            }

            if (credentials != null)
            {
                nameValuePairs.add(new BasicNameValuePair("creds",credentials));
            }

            // do the query
            JsonObject jsonObject = doQuery(query, nameValuePairs );

            JsonObject metrics = jsonObject.getJsonObject("metrics");
            if ( metrics.containsKey("mutationCount") )
            {
                updateCount = metrics.getInt("mutationCount");
                return false;
            }
            if ( metrics.containsKey("resultCount") && metrics.getInt("resultCount") > 0 )
            {
                resultSet = new CBResultSet(jsonObject);
                return true;
            }


        }
        catch (Exception ex)
        {
            logger.error ("Error executing update query {} {}", query, ex.getMessage());
            throw new SQLException("Error executing update",ex.getCause());
        }
        return false;
    }

    public JsonObject prepareStatement( String sql ) throws SQLException
    {
        List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();

        nameValuePairs.add(new BasicNameValuePair("statement", "prepare " + sql));
        if ( queryTimeout != 0 )
        {
            nameValuePairs.add(new BasicNameValuePair("timeout", ""+queryTimeout+'s'));
        }

        return doQuery(sql, nameValuePairs);
    }

    public int [] executeBatch() throws SQLException
    {
        try
        {
            HttpPost httpPost = new HttpPost(cluster.getNextEndpoint());
            httpPost.setHeader("Accept", "application/json");

            List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();

            for (String query:batchStatements)
            {
                nameValuePairs.add(new BasicNameValuePair("statement", query));
            }
            if ( queryTimeout != 0 )
            {
                nameValuePairs.add(new BasicNameValuePair("timeout", ""+queryTimeout+'s'));
            }
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
                    if ( metrics.containsKey("mutationCount") )
                    {
                        updateCount = metrics.getInt("mutationCount");
                        return new int [0];
                    }
                    if ( metrics.containsKey("resultCount") )
                    {
                        resultSet = new CBResultSet(jsonObject);
                        return new int [0];
                    }
                }
                else if (statusString.equals("running"))
                {
                    return new int [0];
                }
                else if (statusString.equals("completed"))
                {
                    return new int [0];
                }
                else if (statusString.equals("stopped"))
                {
                    return new int [0];
                }
                else if (statusString.equals("timeout"))
                {
                    return new int [0];
                }
                else if (statusString.equals("fatal"))
                {
                    return new int [0];
                }

                else
                {
                    //logger.error("Unexpected status string {} for query {}", statusString, query);
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
            //logger.error ("Error executing update query {} {}", query, ex.getMessage());
            throw new SQLException("Error executing update",ex.getCause());
        }
        return new int [0];

    }
    public void addBatch( String query ) throws SQLException
    {
        batchStatements.add(query);
    }
    public int getUpdateCount()
    {
        return updateCount;
    }
    public CBResultSet getResultSet()
    {
        return resultSet;
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

    public void setQueryTimeout( int seconds ) throws SQLException
    {
        this.queryTimeout =  seconds;
    }
    public int getQueryTimeout( ) throws SQLException
    {
        return queryTimeout;
    }
    public void close() throws Exception
    {
        httpClient.close();
    }
}
