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

import com.couchbase.CBResultSet;
import com.couchbase.CBStatement;
import com.couchbase.jdbc.core.CouchResponse;
import org.apache.http.NameValuePair;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

/**
 * Created by davec on 2015-02-26.
 */
public interface Protocol
{
    public void connect() throws Exception;
    public void close() throws Exception;

    public CBResultSet query( CBStatement statement, String sql ) throws SQLException;
    public int executeUpdate( CBStatement statement, String sql ) throws SQLException;
    public boolean execute( CBStatement statement, String sql ) throws SQLException;
    public void addBatch( String sql ) throws SQLException;
    public void clearBatch() throws SQLException;
    public int [] executeBatch(  ) throws SQLException;

    public CouchResponse prepareStatement( String sql, String []returning ) throws SQLException;
    public CouchResponse doQuery(String query, List<NameValuePair> nameValuePairs ) throws SQLException;

    public long getUpdateCount();
    public CBResultSet getResultSet();

    public String getURL();
    public String getUserName();
    public String getPassword();

    public void setConnectionTimeout(String timeout);
    public void setConnectionTimeout(int timeout);
    public void setReadOnly(boolean readOnly);
    public boolean getReadOnly();

    public void setQueryTimeout(int seconds) throws SQLException;
    public int getQueryTimeout() throws SQLException;
    public SQLWarning getWarnings() throws SQLException;
    public void clearWarning() throws SQLException;
    public void setSchema(String schema) throws SQLException;
    public String getSchema() throws SQLException;
    public boolean isValid(int timeout) throws SQLException;

}
