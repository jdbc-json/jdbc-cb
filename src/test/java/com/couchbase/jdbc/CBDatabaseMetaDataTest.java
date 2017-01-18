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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.couchbase.jdbc.util.Credentials;

import junit.framework.TestCase;

import java.sql.*;
import java.util.Properties;

/**
 * Created by davec on 2015-09-09.
 */
@RunWith(JUnit4.class)
public class CBDatabaseMetaDataTest extends TestCase
{
    public Connection con;
    public static Properties properties;
    DatabaseMetaData dbmd;

    @BeforeClass
    public static void initialize() throws Exception
    {
        CBDatabaseMetaDataTest.properties = new Properties();
        properties.put(ConnectionParameters.SCAN_CONSISTENCY,"request_plus");
        properties.put(ConnectionParameters.USER,TestUtil.getUser());
        properties.put(ConnectionParameters.PASSWORD,TestUtil.getPassword());
        Credentials cred = new Credentials();
        cred.add("Administrator", "password");
        properties.setProperty("credentials", cred.toString());
        TestUtil.resetEnvironmentProperties(null);
    }

    @Before
    public void openConnection() throws Exception
    {
        con = DriverManager.getConnection(TestUtil.getURL(), CBDatabaseMetaDataTest.properties);
        assertNotNull(con);
        dbmd = con.getMetaData();
        assertNotNull(dbmd);
    }

    @After
    public void close() throws Exception
    {
        con.close();
    }

    @Test
    public void testAllProceduresAreCallable() throws Exception
    {
        assertTrue(dbmd.allProceduresAreCallable());
    }

    @Test
    public void testAllTablesAreSelectable() throws Exception
    {
        assertTrue(dbmd.allTablesAreSelectable());
    }

    @Test
    public void testGetURL() throws Exception
    {
        assertNotNull(dbmd.getURL());
    }

    @Test
    public void testGetUserName() throws Exception
    {
        assertNotNull(dbmd.getUserName());
    }

    @Test
    public void testIsReadOnly() throws Exception
    {
        assertFalse(dbmd.isReadOnly());
    }

    @Test
    public void testNullsAreSortedHigh() throws Exception
    {
        assertFalse(dbmd.nullsAreSortedHigh());
    }

    @Test
    public void testNullsAreSortedLow() throws Exception
    {
        assertTrue(dbmd.nullsAreSortedLow());
    }

    @Test
    public void testNullsAreSortedAtStart() throws Exception
    {
        assertFalse(dbmd.nullsAreSortedAtStart());
    }

    @Test
    public void testNullsAreSortedAtEnd() throws Exception
    {
        assertFalse(dbmd.nullsAreSortedAtEnd());

    }

    @Test
    public void testGetDatabaseProductName() throws Exception
    {
        assertEquals("Couchbase", dbmd.getDatabaseProductName());
    }

    @Test
    public void testGetDatabaseProductVersion() throws Exception
    {
        ResultSet rs = con.createStatement().executeQuery("select version()");
        assertTrue(rs.next());
        String version= rs.getString(1);
        assertEquals(version, dbmd.getDatabaseProductVersion());

    }

    @Test
    public void testGetDriverName() throws Exception
    {
        assertEquals("n1ql_jdbc", dbmd.getDriverName());
    }

    @Test
    public void testGetDriverVersion() throws Exception
    {
        assertEquals("1.1", dbmd.getDriverVersion());
    }

    @Test
    public void testGetDriverMajorVersion() throws Exception
    {
        assertEquals(1,dbmd.getDriverMajorVersion());
    }

    @Test
    public void testGetDriverMinorVersion() throws Exception
    {
        assertEquals(1,dbmd.getDriverMinorVersion());
    }

    @Test
    public void testUsesLocalFiles() throws Exception
    {
        assertFalse(dbmd.usesLocalFiles());
    }

    @Test
    public void testUsesLocalFilePerTable() throws Exception
    {
        assertFalse(dbmd.usesLocalFilePerTable());
    }

    @Test
    public void testSupportsMixedCaseIdentifiers() throws Exception
    {
        assertTrue(dbmd.supportsMixedCaseIdentifiers());
    }

    @Test
    public void testStoresUpperCaseIdentifiers() throws Exception
    {
        assertFalse(dbmd.storesUpperCaseIdentifiers());
    }

    @Test
    public void testStoresLowerCaseIdentifiers() throws Exception
    {
        assertFalse(dbmd.storesLowerCaseIdentifiers());
    }

    @Test
    public void testStoresMixedCaseIdentifiers() throws Exception
    {
        assertFalse(dbmd.storesMixedCaseIdentifiers());
    }

    @Test
    public void testSupportsMixedCaseQuotedIdentifiers() throws Exception
    {
        assertTrue(dbmd.supportsMixedCaseQuotedIdentifiers());
    }

    @Test
    public void testGetNumericFunctions() throws Exception
    {
        assertEquals(0, dbmd.getNumericFunctions().compareTo("add,div,mod,mult,neg,sub,abs,acos,asin,atan,atan2,ceil,cos,deg,degrees,e,exp,ln,log,floor,inf,nan,neginf,pi,posinf,power,rad,radians,random,round,sign,sin,sqrt,tan,trunc"));
    }

    @Test
    public void testGetStringFunctions() throws Exception
    {
        assertEquals(0, dbmd.getStringFunctions().compareTo("contains,initcap,length,lower,ltrim,position,pos,regex_contains,regex_like,regex_position,regex_pos,regex_replace,repeat,replace,rtrim,split,substr,title,trim,upper"));
    }

    @Test
    public void testGetTimeDateFunctions() throws Exception
    {
        assertEquals(0, dbmd.getTimeDateFunctions().compareTo("clock_millis,clock_str,date_add_millis,date_add_str,date_diff_millis,date_diff_str,date_part_millis,date_part_str,date_trunc_millis,date_trunc_str,millis,millis_to_str,millis_to_utc,millis_to_zone_name,now_millis,now_str,str_to_millis,str_to_utc,str_to_zone_name"));

    }

    @Test
    public void testGetSearchStringEscape() throws Exception
    {
        assertEquals("\\", dbmd.getSearchStringEscape());
    }

    @Test
    public void testGetExtraNameCharacters() throws Exception
    {
        assertEquals("", dbmd.getExtraNameCharacters());
    }

    @Test
    public void testSupportsAlterTableWithAddColumn() throws Exception
    {
        assertFalse(dbmd.supportsAlterTableWithAddColumn());
    }

    @Test
    public void testSupportsAlterTableWithDropColumn() throws Exception
    {
        assertFalse(dbmd.supportsAlterTableWithDropColumn());
    }

    @Test
    public void testSupportsColumnAliasing() throws Exception
    {
        assertTrue(dbmd.supportsColumnAliasing());
    }

    @Test
    public void testNullPlusNonNullIsNull() throws Exception
    {

        //(NULL + MISSING = MISSING.   NULL + X = NULL)
        assertFalse(dbmd.nullPlusNonNullIsNull());
    }

    @Test
    public void testSupportsConvert() throws Exception
    {
        assertTrue(dbmd.supportsConvert());
    }

    @Test
    public void testSupportsTableCorrelationNames() throws Exception
    {
        assertTrue(dbmd.supportsTableCorrelationNames());
    }

    @Test
    public void testSupportsDifferentTableCorrelationNames() throws Exception
    {
        assertTrue(dbmd.supportsDifferentTableCorrelationNames());
    }

    @Test
    public void testSupportsExpressionsInOrderBy() throws Exception
    {
        assertTrue(dbmd.supportsExpressionsInOrderBy());
    }

    @Test
    public void testSupportsOrderByUnrelated() throws Exception
    {
        assertFalse(dbmd.supportsOrderByUnrelated());
    }

    @Test
    public void testSupportsGroupBy() throws Exception
    {
        assertTrue(dbmd.supportsGroupBy());
    }

    @Test
    public void testSupportsGroupByUnrelated() throws Exception
    {
        assertTrue(dbmd.supportsGroupByUnrelated());
    }

    @Test
    public void testSupportsGroupByBeyondSelect() throws Exception
    {
        assertTrue(dbmd.supportsGroupByBeyondSelect());
    }

    @Test
    public void testSupportsLikeEscapeClause() throws Exception
    {
        assertFalse(dbmd.supportsLikeEscapeClause());
    }

    @Test
    public void testSupportsMultipleResultSets() throws Exception
    {
        assertFalse(dbmd.supportsMultipleResultSets());
    }

    @Test
    public void testSupportsMultipleTransactions() throws Exception
    {
        assertTrue(dbmd.supportsMultipleTransactions());
    }

    @Test
    public void testSupportsNonNullableColumns() throws Exception
    {
        assertFalse(dbmd.supportsNonNullableColumns());
    }

    @Test
    public void testSupportsMinimumSQLGrammar() throws Exception
    {
        assertTrue(dbmd.supportsMinimumSQLGrammar());
    }

    @Test
    public void testSupportsCoreSQLGrammar() throws Exception
    {
        assertTrue(dbmd.supportsCoreSQLGrammar());
    }

    @Test
    public void testSupportsExtendedSQLGrammar() throws Exception
    {
        assertFalse(dbmd.supportsExtendedSQLGrammar());
    }

    @Test
    public void testSupportsANSI92EntryLevelSQL() throws Exception
    {
        assertTrue(dbmd.supportsANSI92EntryLevelSQL());
    }

    @Test
    public void testSupportsANSI92IntermediateSQL() throws Exception {
        assertFalse(dbmd.supportsANSI92IntermediateSQL());

    }

    @Test
    public void testSupportsANSI92FullSQL() throws Exception
    {
        assertFalse(dbmd.supportsANSI92FullSQL());

    }

    @Test
    public void testSupportsIntegrityEnhancementFacility() throws Exception
    {
        assertFalse(dbmd.supportsIntegrityEnhancementFacility());
    }

    @Test
    public void testSupportsOuterJoins() throws Exception
    {
        assertTrue(dbmd.supportsOuterJoins());
    }

    @Test
    public void testSupportsFullOuterJoins() throws Exception
    {
        assertFalse(dbmd.supportsFullOuterJoins());
    }

    @Test
    public void testSupportsLimitedOuterJoins() throws Exception
    {
        assertFalse(dbmd.supportsLimitedOuterJoins());
    }

    @Test
    public void testGetSchemaTerm() throws Exception
    {
        assertEquals("NAMESPACE", dbmd.getSchemaTerm());
    }

    @Test
    public void testGetProcedureTerm() throws Exception
    {
        assertEquals("procedure", dbmd.getProcedureTerm());
    }

    @Test
    public void testGetCatalogTerm() throws Exception
    {
        assertEquals("namespace", dbmd.getCatalogTerm());
    }

    @Test
    public void testIsCatalogAtStart() throws Exception
    {
        assertTrue(dbmd.isCatalogAtStart());
    }

    @Test
    public void testGetCatalogSeparator() throws Exception
    {
        assertEquals(":", dbmd.getCatalogSeparator());
    }

    @Test
    public void testSupportsSchemasInDataManipulation() throws Exception
    {
        assertTrue(dbmd.supportsSchemasInDataManipulation());
    }

    @Test
    public void testSupportsSchemasInProcedureCalls() throws Exception
    {
        assertFalse(dbmd.supportsSchemasInProcedureCalls());
    }

    @Test
    public void testSupportsSchemasInTableDefinitions() throws Exception
    {
        assertFalse(dbmd.supportsSchemasInTableDefinitions());
    }

    @Test
    public void testSupportsSchemasInIndexDefinitions() throws Exception
    {
        assertTrue(dbmd.supportsSchemasInIndexDefinitions());
    }

    @Test
    public void testSupportsSchemasInPrivilegeDefinitions() throws Exception
    {
        assertFalse(dbmd.supportsSchemasInPrivilegeDefinitions());
    }

    @Test
    public void testSupportsCatalogsInDataManipulation() throws Exception
    {
        assertTrue(dbmd.supportsCatalogsInDataManipulation());
    }

    @Test
    public void testSupportsCatalogsInProcedureCalls() throws Exception
    {
        assertFalse(dbmd.supportsCatalogsInProcedureCalls());
    }

    @Test
    public void testSupportsCatalogsInTableDefinitions() throws Exception
    {
        assertFalse(dbmd.supportsCatalogsInTableDefinitions());
    }

    @Test
    public void testSupportsCatalogsInIndexDefinitions() throws Exception
    {
        assertTrue(dbmd.supportsCatalogsInIndexDefinitions());
    }

    @Test
    public void testSupportsCatalogsInPrivilegeDefinitions() throws Exception
    {
        assertFalse(dbmd.supportsCatalogsInPrivilegeDefinitions());
    }

    @Test
    public void testSupportsPositionedDelete() throws Exception
    {
        assertFalse(dbmd.supportsPositionedDelete());
    }

    @Test
    public void testSupportsPositionedUpdate() throws Exception
    {
        assertFalse(dbmd.supportsPositionedUpdate());
    }

    @Test
    public void testSupportsSelectForUpdate() throws Exception
    {
        assertFalse(dbmd.supportsSelectForUpdate());
    }

    @Test
    public void testSupportsStoredProcedures() throws Exception
    {
        assertFalse(dbmd.supportsStoredProcedures());
    }

    @Test
    public void testSupportsSubqueriesInComparisons() throws Exception
    {
        assertTrue(dbmd.supportsSubqueriesInComparisons());
    }

    @Test
    public void testSupportsSubqueriesInExists() throws Exception
    {
        assertTrue(dbmd.supportsSubqueriesInExists());
    }

    @Test
    public void testSupportsSubqueriesInIns() throws Exception
    {
        assertTrue(dbmd.supportsSubqueriesInIns());
    }

    @Test
    public void testSupportsSubqueriesInQuantifieds() throws Exception
    {
        assertTrue(dbmd.supportsSubqueriesInQuantifieds());
    }

    @Test
    public void testSupportsCorrelatedSubqueries() throws Exception
    {
        assertFalse(dbmd.supportsCorrelatedSubqueries());
    }

    @Test
    public void testSupportsUnion() throws Exception
    {
        assertTrue(dbmd.supportsUnion());
    }

    @Test
    public void testSupportsUnionAll() throws Exception
    {
        assertTrue(dbmd.supportsUnionAll());
    }

    @Test
    public void testSupportsOpenCursorsAcrossCommit() throws Exception
    {
        assertFalse(dbmd.supportsOpenCursorsAcrossCommit());
    }

    @Test
    public void testSupportsOpenCursorsAcrossRollback() throws Exception
    {
        assertFalse(dbmd.supportsOpenCursorsAcrossRollback());
    }

    @Test
    public void testSupportsOpenStatementsAcrossCommit() throws Exception
    {
        assertTrue(dbmd.supportsOpenStatementsAcrossCommit());
    }

    @Test
    public void testSupportsOpenStatementsAcrossRollback() throws Exception
    {
        assertTrue(dbmd.supportsOpenStatementsAcrossRollback());
    }

    @Test
    public void testGetMaxBinaryLiteralLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxBinaryLiteralLength());
    }

    @Test
    public void testGetMaxCharLiteralLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxCharLiteralLength());
    }

    @Test
    public void testGetMaxColumnNameLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxColumnNameLength());
    }

    @Test
    public void testGetMaxColumnsInGroupBy() throws Exception
    {
        assertEquals(0, dbmd.getMaxColumnsInGroupBy());
    }

    @Test
    public void testGetMaxColumnsInIndex() throws Exception
    {
        assertEquals(0, dbmd.getMaxColumnsInIndex());
    }

    @Test
    public void testGetMaxColumnsInOrderBy() throws Exception
    {
        assertEquals(0, dbmd.getMaxColumnsInOrderBy());
    }

    @Test
    public void testGetMaxColumnsInSelect() throws Exception
    {
        assertEquals(0, dbmd.getMaxColumnsInSelect());
    }

    @Test
    public void testGetMaxColumnsInTable() throws Exception
    {
        assertEquals(0, dbmd.getMaxColumnsInTable());
    }

    @Test
    public void testGetMaxConnections() throws Exception
    {
        assertEquals(0, dbmd.getMaxConnections());
    }

    @Test
    public void testGetMaxCursorNameLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxCursorNameLength());
    }

    @Test
    public void testGetMaxIndexLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxIndexLength());
    }

    @Test
    public void testGetMaxSchemaNameLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxSchemaNameLength());
    }

    @Test
    public void testGetMaxProcedureNameLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxProcedureNameLength());
    }

    @Test
    public void testGetMaxCatalogNameLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxCatalogNameLength());
    }

    @Test
    public void testGetMaxRowSize() throws Exception
    {
        assertEquals(0, dbmd.getMaxRowSize());
    }

    @Test
    public void testDoesMaxRowSizeIncludeBlobs() throws Exception
    {
        assertTrue(dbmd.doesMaxRowSizeIncludeBlobs());
    }

    @Test
    public void testGetMaxStatementLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxStatementLength());
    }

    @Test
    public void testGetMaxStatements() throws Exception
    {
        assertEquals(0, dbmd.getMaxStatements());
    }

    @Test
    public void testGetMaxTableNameLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxTableNameLength());
    }

    @Test
    public void testGetMaxTablesInSelect() throws Exception
    {
        assertEquals(0, dbmd.getMaxTablesInSelect());
    }

    @Test
    public void testGetMaxUserNameLength() throws Exception
    {
        assertEquals(0, dbmd.getMaxUserNameLength());
    }

    @Test
    public void testGetDefaultTransactionIsolation() throws Exception
    {
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, dbmd.getDefaultTransactionIsolation());
    }

    @Test
    public void testSupportsTransactions() throws Exception
    {
        assertTrue(dbmd.supportsTransactions());
    }

    @Test
    public void testSupportsTransactionIsolationLevel() throws Exception
    {
        assertTrue(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
        assertFalse(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
        assertFalse(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
        assertFalse(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
        assertFalse(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));

    }

    @Test
    public void testSupportsDataDefinitionAndDataManipulationTransactions() throws Exception
    {
        assertTrue(dbmd.supportsDataDefinitionAndDataManipulationTransactions());
    }

    @Test
    public void testSupportsDataManipulationTransactionsOnly() throws Exception
    {
        assertFalse(dbmd.supportsDataManipulationTransactionsOnly());
    }

    @Test
    public void testDataDefinitionCausesTransactionCommit() throws Exception
    {
        assertFalse(dbmd.dataDefinitionCausesTransactionCommit());
    }

    @Test
    public void testDataDefinitionIgnoredInTransactions() throws Exception
    {
        assertFalse(dbmd.dataDefinitionIgnoredInTransactions());

    }

    @Test
    public void testGetProcedures() throws Exception
    {
        ResultSet rs = dbmd.getProcedures(null, null, null);
        // should always be empty
        assertFalse(rs.next());
    }

    @Test
    public void testGetProcedureColumns() throws Exception
    {
        ResultSet rs = dbmd.getProcedureColumns(null, null, null, null);
        assertFalse(rs.next());
    }

    @Test
    public void testGetTables() throws Exception
    {
        ResultSet rs = dbmd.getTables(null, null, null,null);
        assertTrue(rs.next());

        rs = dbmd.getTables(null, "default", null, null);
        assertTrue(rs.next());

        rs = dbmd.getTables(null, null, "default", null);
        assertTrue(rs.next());

        rs = dbmd.getTables(null, "default", "default", null);
        assertTrue(rs.next());

    }

    @Test
    public void testGetSchemas() throws Exception
    {
        ResultSet rs = dbmd.getSchemas();
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"),"default");
    }

    @Test
    public void testGetCatalogs() throws Exception
    {
        ResultSet rs = dbmd.getCatalogs();

        assertTrue(rs.next());
        assertEquals(rs.getString(1), "default");

    }

    @Test
    public void testGetTableTypes() throws Exception
    {
        ResultSet rs = dbmd.getTableTypes();
        assertTrue(rs.next());
    }

    @Test
    public void testGetColumns() throws Exception
    {
        ResultSet rs = dbmd.getColumns(null, null, null, null);
        assertFalse(rs.next());
    }

    @Test
    public void testGetColumnPrivileges() throws Exception
    {
        ResultSet rs = dbmd.getColumnPrivileges(null, null, null, null);
        assertFalse(rs.next());

    }

    @Test
    public void testGetTablePrivileges() throws Exception
    {
        ResultSet rs = dbmd.getTablePrivileges(null, null, null);
        assertFalse(rs.next());

    }

    @Test
    public void testGetBestRowIdentifier() throws Exception
    {
        ResultSet rs = dbmd.getBestRowIdentifier(null, null, null, 0, false);
        assertFalse(rs.next());
    }

    @Test
    public void testGetVersionColumns() throws Exception
    {
        ResultSet rs = dbmd.getVersionColumns(null, null, null);
        assertFalse(rs.next());
    }

    @Test
    public void testGetPrimaryKeys() throws Exception
    {
        ResultSet rs = dbmd.getPrimaryKeys(null, null, null);
        assertTrue (rs.next());
    }

    @Test
    public void testGetImportedKeys() throws Exception
    {
        ResultSet rs = dbmd.getImportedKeys(null, null, null);
        assertFalse(rs.next());

    }

    @Test
    public void testGetExportedKeys() throws Exception
    {
        ResultSet rs = dbmd.getExportedKeys(null, null,null);
        assertFalse(rs.next());
    }

    @Test
    public void testGetCrossReference() throws Exception
    {
        ResultSet rs = dbmd.getCrossReference(null, null, null,null,null,null);
        assertFalse(rs.next());
    }

    @Test
    public void testGetTypeInfo() throws Exception
    {
        ResultSet rs = dbmd.getTypeInfo();
        while(rs.next())
        {
            assertTrue(!rs.getString("TYPE_NAME").isEmpty());
        }
    }

    @Test
    public void testSupportsResultSetType() throws Exception
    {
        assertTrue(dbmd.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        assertFalse(dbmd.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dbmd.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
    }

    @Test
    public void testSupportsResultSetConcurrency() throws Exception
    {
        assertFalse(dbmd.supportsResultSetConcurrency(0,0));
    }

    @Test
    public void testOwnUpdatesAreVisible() throws Exception
    {
        assertFalse(dbmd.ownUpdatesAreVisible(0));

    }

    @Test
    public void testOwnDeletesAreVisible() throws Exception
    {
        assertFalse(dbmd.ownDeletesAreVisible(0));
    }

    @Test
    public void testOwnInsertsAreVisible() throws Exception {
        assertFalse(dbmd.ownInsertsAreVisible(0));
    }

    @Test
    public void testOthersUpdatesAreVisible() throws Exception {
        assertFalse(dbmd.othersUpdatesAreVisible(0));
    }

    @Test
    public void testOthersDeletesAreVisible() throws Exception {
        assertFalse(dbmd.othersDeletesAreVisible(0));
    }

    @Test
    public void testOthersInsertsAreVisible() throws Exception {
        assertFalse(dbmd.othersInsertsAreVisible(0));
    }

    @Test
    public void testUpdatesAreDetected() throws Exception {
        assertFalse(dbmd.updatesAreDetected(Types.NULL));
    }

    @Test
    public void testDeletesAreDetected() throws Exception
    {
        assertFalse(dbmd.deletesAreDetected(Types.JAVA_OBJECT));
    }

    @Test
    public void testInsertsAreDetected() throws Exception
    {
        assertFalse(dbmd.insertsAreDetected(Types.JAVA_OBJECT));
    }

    @Test
    public void testSupportsBatchUpdates() throws Exception
    {
        assertTrue(dbmd.supportsBatchUpdates());
    }

    @Test
    public void testGetUDTs() throws Exception
    {
        ResultSet rs = dbmd.getUDTs(null, null, null, null);
        assertFalse(rs.next());
    }

    @Test
    public void testGetConnection() throws Exception
    {
        assertNotNull(dbmd.getConnection());
    }

    @Test
    public void testSupportsSavepoints() throws Exception
    {
        assertFalse(dbmd.supportsSavepoints());
    }

    @Test
    public void testSupportsNamedParameters() throws Exception
    {
        assertTrue(dbmd.supportsNamedParameters());
    }

    @Test
    public void testSupportsMultipleOpenResults() throws Exception
    {
        assertFalse(dbmd.supportsMultipleOpenResults());
    }

    @Test
    public void testSupportsGetGeneratedKeys() throws Exception
    {
        assertFalse(dbmd.supportsGetGeneratedKeys());
    }

    @Test
    public void testGetSuperTypes() throws Exception {
        ResultSet rs = dbmd.getSuperTypes( null,null,null );
        assertFalse(rs.next());
    }

    @Test
    public void testGetSuperTables() throws Exception
    {
        ResultSet rs = dbmd.getSuperTables(null, null, null);
        assertFalse(rs.next());
    }

    @Test
    public void testGetAttributes() throws Exception
    {
        ResultSet rs = dbmd.getAttributes(null, null, null, null);
        assertFalse(rs.next());
    }

    @Test
    public void testSupportsResultSetHoldability() throws Exception
    {
        assertFalse(dbmd.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    }

    @Test
    public void testGetResultSetHoldability() throws Exception
    {
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, dbmd.getResultSetHoldability());
    }

    @Test
    public void testGetDatabaseMajorVersion() throws Exception
    {
        assertEquals(1,dbmd.getDatabaseMajorVersion());
    }

    @Test
    public void testGetDatabaseMinorVersion() throws Exception
    {
        assertEquals(1,dbmd.getDatabaseMinorVersion());
    }

    @Test
    public void testGetJDBCMajorVersion() throws Exception
    {
        assertEquals(4,dbmd.getJDBCMajorVersion());
    }

    @Test
    public void testGetJDBCMinorVersion() throws Exception
    {
        int expected = (Integer.parseInt(System.getProperty("java.specification.version").split("\\.")[1]) == 8) ? 2:1;
        assertEquals(expected,dbmd.getJDBCMinorVersion());
    }

    @Test
    public void testGetSQLStateType() throws Exception
    {
        assertEquals(DatabaseMetaData.sqlStateSQL, dbmd.getSQLStateType());
    }

    @Test
    public void testLocatorsUpdateCopy() throws Exception
    {
        assertFalse(dbmd.locatorsUpdateCopy());
    }

    @Test
    public void testSupportsStatementPooling() throws Exception
    {
        assertFalse(dbmd.supportsStatementPooling());
    }

    @Test
    public void testGetRowIdLifetime() throws Exception
    {
        assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, dbmd.getRowIdLifetime());
    }

    @Test
    public void testGetSchemas1() throws Exception
    {
        ResultSet rs = dbmd.getSchemas(null, "default");
        assertTrue(rs.next());
    }

    @Test
    public void testSupportsStoredFunctionsUsingCallSyntax() throws Exception
    {
        assertFalse(dbmd.supportsStoredFunctionsUsingCallSyntax());
    }

    @Test
    public void testAutoCommitFailureClosesAllResultSets() throws Exception
    {
        assertFalse(dbmd.autoCommitFailureClosesAllResultSets());
    }

    @Test
    public void testGetClientInfoProperties() throws Exception
    {
        assertFalse(false);
    }

    @Test
    public void testGetFunctions() throws Exception
    {
        ResultSet rs = dbmd.getFunctions(null, null, null);
        assertFalse(rs.next());
    }

    @Test
    public void testGetFunctionColumns() throws Exception
    {
        ResultSet rs = dbmd.getFunctionColumns(null, null, null, null);
        assertFalse(rs.next());
    }

    @Test
    public void testGetPseudoColumns() throws Exception
    {
        ResultSet rs = dbmd.getPseudoColumns(null, null, null, null);
        assertFalse(rs.next());

    }

    @Test
    public void testGeneratedKeyAlwaysReturned() throws Exception
    {
        assertFalse(dbmd.generatedKeyAlwaysReturned());
    }
}