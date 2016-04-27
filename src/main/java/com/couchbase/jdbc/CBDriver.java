
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


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;


public class CBDriver implements java.sql.Driver
{
    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(CBDriver.class.getName());

    public static final int MAJOR_VERSION = 1;

    public static final int MINOR_VERSION = 1;

    public static final String DRIVER_NAME = "n1ql_jdbc";

    static CBDriver registered;

    final Thread houseKeepingThread;
    final ClusterThread ct;
    static
    {
        try
        {
            registered = new CBDriver();
            java.sql.DriverManager.registerDriver(registered);

        }
        catch (SQLException e)
        {
            logger.error("Error registering driver", e);
        }


    }

    public CBDriver() throws SQLException
    {
        ct = new ClusterThread();
        houseKeepingThread = new Thread(ct, "Couchbase housekeeping thread");
        houseKeepingThread.setDaemon(true);
        houseKeepingThread.start();
    }
    /**
     * Attempts to make a database connection to the given URL.
     * The driver should return "null" if it realizes it is the wrong kind
     * of driver to connect to the given URL.  This will be common, as when
     * the JDBC driver manager is asked to connect to a given URL it passes
     * the URL to each loaded driver in turn.
     * 
     * <P>The driver should throw an <code>SQLException</code> if it is the right
     * driver to connect to the given URL but has trouble connecting to
     * the database.
     * 
     * <P>The <code>java.util.Properties</code> argument can be used to pass
     * arbitrary string tag/value pairs as connection arguments.
     * Normally at least "user" and "password" properties should be
     * included in the <code>Properties</code> object.
     *
     * @param url  the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as
     *             connection arguments. Normally at least a "user" and
     *             "password" property should be included.
     * @return a <code>Connection</code> object that represents a
     * connection to the URL
     * @throws java.sql.SQLException if a database access error occurs
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException
    {

        if (acceptsURL(url))
        {
            CBConnection con = new CBConnection(url, info);
            ct.addConnection(con);
            return con;
        }
        else
        {
            return null;
        }
    }

    /**
     * Retrieves whether the driver thinks that it can open a connection
     * to the given URL.  Typically drivers will return <code>true</code> if they
     * understand the subprotocol specified in the URL and <code>false</code> if
     * they do not.
     *
     * @param url the URL of the database
     * @return <code>true</code> if this driver understands the given URL;
     * <code>false</code> otherwise
     * @throws java.sql.SQLException if a database access error occurs
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException
    {
        return url.startsWith("jdbc:couchbase:");
    }

    /**
     * Gets information about the possible properties for this driver.
     * 
     * The <code>getPropertyInfo</code> method is intended to allow a generic
     * GUI tool to discover what properties it should prompt
     * a human for in order to get
     * enough information to connect to a database.  Note that depending on
     * the values the human has supplied so far, additional values may become
     * necessary, so it may be necessary to iterate though several calls
     * to the <code>getPropertyInfo</code> method.
     *
     * @param url  the URL of the database to which to connect
     * @param info a proposed list of tag/value pairs that will be sent on
     *             connect open
     * @return an array of <code>DriverPropertyInfo</code> objects describing
     * possible properties.  This array may be an empty array if
     * no properties are required.
     * @throws java.sql.SQLException if a database access error occurs
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
    {
        return new DriverPropertyInfo[0];
    }

    /**
     * Retrieves the driver's major version number. Initially this should be 1.
     *
     * @return this driver's major version number
     */
    @Override
    public int getMajorVersion()
    {
        return 0;
    }

    /**
     * Gets the driver's minor version number. Initially this should be 0.
     *
     * @return this driver's minor version number
     */
    @Override
    public int getMinorVersion()
    {
        return 0;
    }

    /**
     * Reports whether this driver is a genuine JDBC
     * Compliant(TM) driver.
     * A driver may only report <code>true</code> here if it passes the JDBC
     * compliance tests; otherwise it is required to return <code>false</code>.
     * 
     * JDBC compliance requires full support for the JDBC API and full support
     * for SQL 92 Entry Level.  It is expected that JDBC compliant drivers will
     * be available for all the major commercial databases.
     * 
     * This method is not intended to encourage the development of non-JDBC
     * compliant drivers, but is a recognition of the fact that some vendors
     * are interested in using the JDBC API and framework for lightweight
     * databases that do not support full database functionality, or for
     * special databases such as document information retrieval where a SQL
     * implementation may not be feasible.
     *
     * @return <code>true</code> if this driver is JDBC Compliant; <code>false</code>
     * otherwise
     */
    @Override
    public boolean jdbcCompliant()
    {
        return false;
    }

    private static final MessageFormat mf = new MessageFormat("Method {0}.{1} is not yet implemented.");

    public static java.sql.SQLFeatureNotSupportedException notImplemented(Class callClass, String functionName)
    {

        return new java.sql.SQLFeatureNotSupportedException(mf.format(new Object [] {callClass.getName(),functionName}));
    }

    public static void setLogLevel(Level logLevel)
    {
        synchronized (CBDriver.class)
        {
            Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.couchbase");
            logger.setLevel(logLevel);
            //logLevelSet = true;
        }
    }

    public static Level getLogLevel()
    {
        synchronized (CBDriver.class)
        {
            Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.couchbase");
            return logger.getLevel();
        }
    }

    /**
     * Return the parent Logger of all the Loggers used by this driver. This
     * should be the Logger farthest from the root Logger that is
     * still an ancestor of all of the Loggers used by this driver. Configuring
     * this Logger will affect all of the log messages generated by the driver.
     * In the worst case, this may be the root Logger.
     *
     * @return the parent Logger for this driver
     * @throws java.sql.SQLFeatureNotSupportedException if the driver does not use <code>java.util.logging</code>.
     * @since 1.7
     */
    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException
    {
        throw notImplemented(CBDriver.class, "getParentLogger");
    }

    public static void cleanup()
    {
        if (registered != null)
        {
            try
            {
                DriverManager.deregisterDriver(registered);

                //stop the thread below
                runCluster=false;
                Thread.currentThread().interrupt();


            }
            catch (SQLException e)
            {
                logger.warn("Error deregistering driver", e);
            }
        }
    }
    public void cleanup(CBConnection con)
    {
        ct.removeConnection(con);
    }

    static boolean runCluster=true;

    private static class ClusterThread implements Runnable
    {

        ConcurrentLinkedQueue <CBConnection> connections;
        ClusterThread()
        {
            connections = new ConcurrentLinkedQueue<CBConnection>();
        }
        @Override
        public void run()
        {
            while(runCluster)
            {
                CBConnection connection = connections.poll();
                if ( connection != null )
                {
                    try
                    {
                        connection.pollCluster();
                    } catch (SQLException e)
                    {
                        logger.error("Error polling cluster", e);
                    }
                }
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e)
                {
                    // ignore it
                }
            }
        }
        public void addConnection(CBConnection connection)
        {
            connections.add(connection);
        }
        public void removeConnection(CBConnection connection)
        {
            connections.remove(connection);
        }
    }
}
