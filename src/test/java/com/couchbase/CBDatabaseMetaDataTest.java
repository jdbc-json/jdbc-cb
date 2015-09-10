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

package com.couchbase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by davec on 2015-09-09.
 */
@RunWith(JUnit4.class)
public class CBDatabaseMetaDataTest extends CouchBaseTestCase
{
    DatabaseMetaData dbmd;
    @Before
    public void getDatabaseMetaData() throws Exception
    {
        super.openConnection();
        dbmd = con.getMetaData();

    }
    @After
    public void close() throws Exception
    {
        con.close();
    }

    @Test
    public void testAllProceduresAreCallable() throws Exception {

    }

    @Test
    public void testAllTablesAreSelectable() throws Exception {

    }

    @Test
    public void testGetURL() throws Exception {

    }

    @Test
    public void testGetUserName() throws Exception {

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
        assertFalse(dbmd.supportsMixedCaseIdentifiers());
    }

    @Test
    public void testStoresUpperCaseIdentifiers() throws Exception
    {
        assertFalse(dbmd.storesUpperCaseIdentifiers());
    }

    @Test
    public void testStoresLowerCaseIdentifiers() throws Exception
    {
        assertTrue(dbmd.storesLowerCaseIdentifiers());
    }

    @Test
    public void testStoresMixedCaseIdentifiers() throws Exception {

    }

    @Test
    public void testSupportsMixedCaseQuotedIdentifiers() throws Exception {

    }

    @Test
    public void testStoresUpperCaseQuotedIdentifiers() throws Exception {

    }

    @Test
    public void testStoresLowerCaseQuotedIdentifiers() throws Exception {

    }

    @Test
    public void testStoresMixedCaseQuotedIdentifiers() throws Exception {

    }

    @Test
    public void testGetIdentifierQuoteString() throws Exception {

    }

    @Test
    public void testGetSQLKeywords() throws Exception {

    }

    @Test
    public void testGetNumericFunctions() throws Exception {

    }

    @Test
    public void testGetStringFunctions() throws Exception {

    }

    @Test
    public void testGetSystemFunctions() throws Exception {

    }

    @Test
    public void testGetTimeDateFunctions() throws Exception {

    }

    @Test
    public void testGetSearchStringEscape() throws Exception {

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
    public void testNullPlusNonNullIsNull() throws Exception {

    }

    @Test
    public void testSupportsConvert() throws Exception {

    }

    @Test
    public void testSupportsConvert1() throws Exception {

    }

    @Test
    public void testSupportsTableCorrelationNames() throws Exception
    {
        assertTrue(dbmd.supportsTableCorrelationNames());
    }

    @Test
    public void testSupportsDifferentTableCorrelationNames() throws Exception {

    }

    @Test
    public void testSupportsExpressionsInOrderBy() throws Exception
    {
        assertTrue(dbmd.supportsExpressionsInOrderBy());
    }

    @Test
    public void testSupportsOrderByUnrelated() throws Exception {

    }

    @Test
    public void testSupportsGroupBy() throws Exception
    {
        assertTrue(dbmd.supportsGroupBy());
    }

    @Test
    public void testSupportsGroupByUnrelated() throws Exception {

    }

    @Test
    public void testSupportsGroupByBeyondSelect() throws Exception {

    }

    @Test
    public void testSupportsLikeEscapeClause() throws Exception {

    }

    @Test
    public void testSupportsMultipleResultSets() throws Exception
    {
        assertFalse(dbmd.supportsMultipleResultSets());
    }

    @Test
    public void testSupportsMultipleTransactions() throws Exception {

    }

    @Test
    public void testSupportsNonNullableColumns() throws Exception
    {
        assertFalse(dbmd.supportsNonNullableColumns());
    }

    @Test
    public void testSupportsMinimumSQLGrammar() throws Exception {

    }

    @Test
    public void testSupportsCoreSQLGrammar() throws Exception {

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
        assertTrue(dbmd.supportsANSI92IntermediateSQL() );

    }

    @Test
    public void testSupportsANSI92FullSQL() throws Exception
    {
        assertFalse(dbmd.supportsANSI92FullSQL());

    }

    @Test
    public void testSupportsIntegrityEnhancementFacility() throws Exception {

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
    public void testSupportsLimitedOuterJoins() throws Exception {

    }

    @Test
    public void testGetSchemaTerm() throws Exception {

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
    public void testGetMaxUserNameLength() throws Exception {
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
    public void testSupportsDataDefinitionAndDataManipulationTransactions() throws Exception {

    }

    @Test
    public void testSupportsDataManipulationTransactionsOnly() throws Exception {

    }

    @Test
    public void testDataDefinitionCausesTransactionCommit() throws Exception
    {
    }

    @Test
    public void testDataDefinitionIgnoredInTransactions() throws Exception
    {
        assertFalse(dbmd.dataDefinitionIgnoredInTransactions());

    }

    @Test
    public void testGetProcedures() throws Exception {

    }

    @Test
    public void testGetProcedureColumns() throws Exception {

    }

    @Test
    public void testGetTables() throws Exception {

    }

    @Test
    public void testGetSchemas() throws Exception {

    }

    @Test
    public void testGetCatalogs() throws Exception
    {
        ResultSet rs = dbmd.getCatalogs();

        assertTrue(rs.next());
        assertEquals(rs.getString(1), "default");

        assertTrue(rs.next());
        assertEquals(rs.getString(1),"system");

    }

    @Test
    public void testGetTableTypes() throws Exception {

    }

    @Test
    public void testGetColumns() throws Exception {

    }

    @Test
    public void testGetColumnPrivileges() throws Exception {

    }

    @Test
    public void testGetTablePrivileges() throws Exception {

    }

    @Test
    public void testGetBestRowIdentifier() throws Exception {

    }

    @Test
    public void testGetVersionColumns() throws Exception {

    }

    @Test
    public void testGetPrimaryKeys() throws Exception {

    }

    @Test
    public void testGetImportedKeys() throws Exception {

    }

    @Test
    public void testGetExportedKeys() throws Exception {

    }

    @Test
    public void testGetCrossReference() throws Exception {

    }

    @Test
    public void testGetTypeInfo() throws Exception {

    }

    @Test
    public void testGetIndexInfo() throws Exception {

    }

    @Test
    public void testSupportsResultSetType() throws Exception {

    }

    @Test
    public void testSupportsResultSetConcurrency() throws Exception {

    }

    @Test
    public void testOwnUpdatesAreVisible() throws Exception {

    }

    @Test
    public void testOwnDeletesAreVisible() throws Exception {

    }

    @Test
    public void testOwnInsertsAreVisible() throws Exception {

    }

    @Test
    public void testOthersUpdatesAreVisible() throws Exception {

    }

    @Test
    public void testOthersDeletesAreVisible() throws Exception {

    }

    @Test
    public void testOthersInsertsAreVisible() throws Exception {

    }

    @Test
    public void testUpdatesAreDetected() throws Exception {

    }

    @Test
    public void testDeletesAreDetected() throws Exception
    {
        assertFalse(dbmd.deletesAreDetected(Types.JAVA_OBJECT));
    }

    @Test
    public void testInsertsAreDetected() throws Exception {

    }

    @Test
    public void testSupportsBatchUpdates() throws Exception
    {
        assertTrue(dbmd.supportsBatchUpdates());
    }

    @Test
    public void testGetUDTs() throws Exception {

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
    public void testSupportsNamedParameters() throws Exception {

    }

    @Test
    public void testSupportsMultipleOpenResults() throws Exception {

    }

    @Test
    public void testSupportsGetGeneratedKeys() throws Exception {

    }

    @Test
    public void testGetSuperTypes() throws Exception {

    }

    @Test
    public void testGetSuperTables() throws Exception {

    }

    @Test
    public void testGetAttributes() throws Exception {

    }

    @Test
    public void testSupportsResultSetHoldability() throws Exception {

    }

    @Test
    public void testGetResultSetHoldability() throws Exception {

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
    public void testGetJDBCMinorVersion() throws Exception {
        assertEquals(1,dbmd.getJDBCMinorVersion());
    }

    @Test
    public void testGetSQLStateType() throws Exception {

    }

    @Test
    public void testLocatorsUpdateCopy() throws Exception {

    }

    @Test
    public void testSupportsStatementPooling() throws Exception {

    }

    @Test
    public void testGetRowIdLifetime() throws Exception {

    }

    @Test
    public void testGetSchemas1() throws Exception {

    }

    @Test
    public void testSupportsStoredFunctionsUsingCallSyntax() throws Exception
    {
        assertFalse(dbmd.supportsStoredFunctionsUsingCallSyntax());
    }

    @Test
    public void testAutoCommitFailureClosesAllResultSets() throws Exception {

    }

    @Test
    public void testGetClientInfoProperties() throws Exception {

    }

    @Test
    public void testGetFunctions() throws Exception {

    }

    @Test
    public void testGetFunctionColumns() throws Exception {

    }

    @Test
    public void testGetPseudoColumns() throws Exception {

    }

    @Test
    public void testGeneratedKeyAlwaysReturned() throws Exception
    {
        assertFalse(dbmd.generatedKeyAlwaysReturned());
    }
}