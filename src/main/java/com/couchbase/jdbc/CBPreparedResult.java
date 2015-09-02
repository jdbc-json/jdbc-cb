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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by davec on 2015-07-28.
 */
public class CBPreparedResult
{
    private static final Logger logger = LoggerFactory.getLogger(CBPreparedResult.class);

    String name;
    String encodedPlan;
    Map operator;
    Map signature;

    public CBPreparedResult( Map preparedResult )
    {
        name = (String)preparedResult.get("name");
        encodedPlan = (String)preparedResult.get("encoded_plan");

        operator = (Map)preparedResult.get("operator");
        signature = (Map)preparedResult.get("signature");

        logger.trace("Prepared statement {}\nencoded_plan {}\noperator {}\nsignature {}", name, encodedPlan,  operator, signature);
    }
    public String getName()
    {
        return name;
    }
    public String getEncodedPlan() { return encodedPlan; }
    public Map getSignature()
    {
        return signature;
    }


}
