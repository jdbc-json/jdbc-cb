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

package com.couchbase.jdbc.util;

import com.couchbase.jdbc.core.CouchBaseSQLException;
import com.couchbase.jdbc.core.EscapedFunctions;
import com.couchbase.jdbc.core.Parser;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by davec on 2015-03-12.
 */
public class SqlParser
{
    final ArrayList statementList;
    final ArrayList<String> fragmentList;


    String query;

    public SqlParser(String sql)
    {
        statementList = new ArrayList();
        //noinspection unchecked
        fragmentList = new ArrayList(15);
        query=sql;
    }

    public int getNumFields()
    {
        return fragmentList.size()-1;
    }
    public void parse()
    {
        int parameterIndex = 1;
        int fragmentStart = 0;
        int inParen = 0;


        char[] aChars = query.toCharArray();

        for (int i = 0; i < aChars.length; ++i)
        {
            switch (aChars[i])
            {
                case '\'': // single-quotes
                    i = Parser.parseSingleQuotes(aChars, i);
                    break;

                case '"': // double-quotes
                    i = Parser.parseDoubleQuotes(aChars, i);
                    break;

                case '-': // possibly -- style comment
                    i = Parser.parseLineComment(aChars, i);
                    break;

                case '/': // possibly /* */ style comment
                    i = Parser.parseBlockComment(aChars, i);
                    break;

                case '$': // possibly dollar quote start
                    i = Parser.parseDollarQuotes(aChars, i);
                    break;

                case '(':
                    inParen++;
                    break;

                case ')':
                    inParen--;
                    break;

                case '?':
                    fragmentList.add(query.substring(fragmentStart, i)+"$"+parameterIndex++);
                    fragmentStart = i + 1;
                    break;

                case ';':
                    if (inParen == 0)
                    {
                        fragmentList.add(query.substring(fragmentStart, i));
                        fragmentStart = i + 1;
                        if (fragmentList.size() > 1 || fragmentList.get(0).trim().length() > 0)
                            //noinspection unchecked
                            statementList.add(fragmentList.toArray(new String[fragmentList.size()]));
                        fragmentList.clear();
                    }
                    break;

                default:
                    break;
            }
        }
        fragmentList.add(query.substring(fragmentStart));

    }
    public String replaceProcessing(String p_sql, boolean replaceProcessingEnabled) throws SQLException
    {
        if (replaceProcessingEnabled)
        {
            // Since escape codes can only appear in SQL CODE, we keep track
            // of if we enter a string or not.
            int len = p_sql.length();
            StringBuilder newsql = new StringBuilder(len);
            int i=0;
            while (i<len){
                i = parseSql(p_sql, i, newsql, false);
                // We need to loop here in case we encounter invalid
                // SQL, consider: SELECT a FROM t WHERE (1 > 0)) ORDER BY a
                // We can't ending replacing after the extra closing paren
                // because that changes a syntax error to a valid query
                // that isn't what the user specified.
                if (i < len) {
                    newsql.append(p_sql.charAt(i));
                    i++;
                }
            }
            return newsql.toString();
        }
        else
        {
            return p_sql;
        }
    }

    private static final short IN_SQLCODE = 0;
    private static final short IN_STRING = 1;
    private static final short IN_IDENTIFIER = 6;
    private static final short BACKSLASH = 2;
    private static final short ESC_TIMEDATE = 3;
    private static final short ESC_FUNCTION = 4;
    private static final short ESC_OUTERJOIN = 5;
    private static final short ESC_ESCAPECHAR = 7;

    /*
     * parse the given sql from index i, appending it to the gven buffer
     * until we hit an unmatched right parentheses or end of string.  When
     * the stopOnComma flag is set we also stop processing when a comma is
     * found in sql text that isn't inside nested parenthesis.
     *
     * @param p_sql the original query text
     * @param i starting position for replacing
     * @param newsql where to write the replaced output
     * @param stopOnComma should we stop after hitting the first comma in sql text?
     * @return the position we stopped processing at
     */
    protected int parseSql(String p_sql,int i,StringBuilder newsql, boolean stopOnComma ) throws SQLException
    {
        short state = IN_SQLCODE;
        int len = p_sql.length();
        int nestedParenthesis=0;
        boolean endOfNested=false;

        // because of the ++i loop
        i--;
        while (!endOfNested && ++i < len)
        {
            char c = p_sql.charAt(i);
            switch (state)
            {
                case IN_SQLCODE:
                    if (c == '\'')      // start of a string?
                        state = IN_STRING;
                    else if (c == '"')      // start of a identifer?
                        state = IN_IDENTIFIER;
                    else if (c=='(') { // begin nested sql
                        nestedParenthesis++;
                    } else if (c==')') { // end of nested sql
                        nestedParenthesis--;
                        if (nestedParenthesis<0){
                            endOfNested=true;
                            break;
                        }
                    } else if (stopOnComma && c==',' && nestedParenthesis==0) {
                        endOfNested=true;
                        break;
                    } else if (c == '{') {     // start of an escape code?
                        if (i + 1 < len)
                        {
                            char next = p_sql.charAt(i + 1);
                            char nextnext = (i + 2 < len) ? p_sql.charAt(i + 2) : '\0';
                            if (next == 'd' || next == 'D')
                            {
                                state = ESC_TIMEDATE;
                                i++;
                                newsql.append("DATE ");
                                break;
                            }
                            else if (next == 't' || next == 'T')
                            {
                                state = ESC_TIMEDATE;
                                if (nextnext == 's' || nextnext == 'S'){
                                    // timestamp constant
                                    i+=2;
                                    //newsql.append("TIMESTAMP ");
                                }else{
                                    // time constant
                                    i++;
                                    //newsql.append("TIME ");
                                }
                                break;
                            }
                            else if ( next == 'f' || next == 'F' )
                            {
                                state = ESC_FUNCTION;
                                i += (nextnext == 'n' || nextnext == 'N') ? 2 : 1;
                                break;
                            }
                            else if ( next == 'o' || next == 'O' )
                            {
                                state = ESC_OUTERJOIN;
                                i += (nextnext == 'j' || nextnext == 'J') ? 2 : 1;
                                break;
                            }
                            else if ( next == 'e' || next == 'E' )
                            { // we assume that escape is the only escape sequence beginning with e
                                state = ESC_ESCAPECHAR;
                                break;
                            }
                        }
                    }
                    newsql.append(c);
                    break;

                case IN_STRING:
                    if (c == '\'')       // end of string?
                        state = IN_SQLCODE;
                    else if (c == '\\' )      // a backslash?
                        state = BACKSLASH;

                    newsql.append(c);
                    break;

                case IN_IDENTIFIER:
                    if (c == '"')       // end of identifier
                        state = IN_SQLCODE;
                    newsql.append(c);
                    break;

                case BACKSLASH:
                    state = IN_STRING;

                    newsql.append(c);
                    break;

                case ESC_FUNCTION:
                    // extract function name
                    String functionName;
                    int posArgs = p_sql.indexOf('(',i);
                    if (posArgs!=-1){
                        functionName=p_sql.substring(i,posArgs).trim();
                        // extract arguments
                        i= posArgs+1;// we start the scan after the first (
                        StringBuilder args=new StringBuilder();
                        i = parseSql(p_sql,i,args,false);
                        // translate the function and parse arguments
                        newsql.append(escapeFunction(functionName,args.toString()));
                    }
                    // go to the end of the function copying anything found
                    i++;
                    while (i<len && p_sql.charAt(i)!='}')
                        newsql.append(p_sql.charAt(i++));
                    state = IN_SQLCODE; // end of escaped function (or query)
                    break;
                case ESC_TIMEDATE:
                case ESC_OUTERJOIN:
                case ESC_ESCAPECHAR:
                    if (c == '}')
                        state = IN_SQLCODE;    // end of escape code.
                    else
                        newsql.append(c);
                    break;
            } // end switch
        }
        return i;
    }

    /*
     * generate sql for escaped functions
     * @param functionName the escaped function name
     * @param args the arguments for this functin
     * @param stdStrings whether standard_conforming_strings is on
     * @return the right postgreSql sql
     */

    protected  String escapeFunction(String functionName, String args) throws SQLException{
        // parse function arguments
        int len = args.length();
        int i=0;
        ArrayList parsedArgs = new ArrayList();
        while (i<len){
            StringBuilder arg = new StringBuilder();
            int lastPos=i;
            i=parseSql(args,i,arg,true );
            if (lastPos!=i){
                parsedArgs.add(arg);
            }
            i++;
        }
        // we can now translate escape functions
        try{
            Method escapeMethod = EscapedFunctions.getFunction(functionName);
            return (String) escapeMethod.invoke(null,new Object[] {parsedArgs});
        }catch(InvocationTargetException e){
            if (e.getTargetException() instanceof SQLException)
                throw (SQLException) e.getTargetException();
            else
                throw new CouchBaseSQLException( e.getTargetException().getMessage());
        }catch (Exception e){
            // by default the function name is kept unchanged
            StringBuilder buf = new StringBuilder();
            buf.append(functionName).append('(');
            for (int iArg = 0;iArg<parsedArgs.size();iArg++){
                buf.append(parsedArgs.get(iArg));
                if (iArg!=(parsedArgs.size()-1))
                    buf.append(',');
            }
            buf.append(')');
            return buf.toString();
        }
    }



    public String toString()
    {
        StringBuffer sbuf = new StringBuffer();

        if ( fragmentList == null ) return "";

        for (String fragment:fragmentList)
        {
            sbuf.append(fragment);
        }
        return sbuf.toString();
    }
}
