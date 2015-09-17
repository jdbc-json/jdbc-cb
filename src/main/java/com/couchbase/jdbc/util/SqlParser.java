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

import com.couchbase.jdbc.core.Parser;

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
