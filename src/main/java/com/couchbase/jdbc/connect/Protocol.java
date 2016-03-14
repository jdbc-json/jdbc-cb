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

import com.couchbase.jdbc.CBResultSet;
import com.couchbase.jdbc.CBStatement;
import com.couchbase.jdbc.core.CouchResponse;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Map;

/**
 * Created by davec on 2015-02-26.
 */
public interface Protocol
{
    void connect() throws Exception;
    void close() throws Exception;

    CBResultSet query(CBStatement statement, String sql) throws SQLException;
    int executeUpdate(CBStatement statement, String sql) throws SQLException;
    boolean execute(CBStatement statement, String sql) throws SQLException;
    void addBatch(String sql) throws SQLException;
    void clearBatch() throws SQLException;
    int [] executeBatch() throws SQLException;

    CouchResponse prepareStatement(String sql, String[] returning) throws SQLException;
    CouchResponse doQuery(String query, Map queryParameters) throws SQLException;

    long getUpdateCount();
    CBResultSet getResultSet();

    String getURL();
    String getUserName();
    String getPassword();

    void setConnectionTimeout(String timeout);
    void setConnectionTimeout(int timeout);
    void setReadOnly(boolean readOnly);
    boolean getReadOnly();

    void setQueryTimeout(int seconds) throws SQLException;
    int getQueryTimeout() throws SQLException;
    SQLWarning getWarnings() throws SQLException;
    void clearWarning() throws SQLException;
    void setSchema(String schema) throws SQLException;
    String getSchema() throws SQLException;
    boolean isValid(int timeout) throws SQLException;
    void pollCluster() throws SQLException;
}
