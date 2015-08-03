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

import java.util.Map;

/**
 * Created by davec on 2015-07-28.
 */
public class CBPreparedResult
{
    String name;
    Map operator;
    Map signature;

    public CBPreparedResult( Map preparedResult )
    {
        name = (String)preparedResult.get("name");
        operator = (Map)preparedResult.get("operator");
        signature = (Map)preparedResult.get("signature");
    }
    public String getName()
    {
        return name;
    }
    public Map getSignature()
    {
        return signature;
    }

}
