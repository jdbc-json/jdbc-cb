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

/**
 * Created by davec on 2015-06-23.
 */
public class CouchMetrics
{
    String executionTime;
    String elapsedTime;

    long resultCount;
    long errorCount;
    long resultSize;
    long mutationCount;
    long warningCount;

    public String getExecutionTime()
    {
        return executionTime;
    }

    public String getElapsedTime()
    {
        return elapsedTime;
    }

    public long getResultCount()
    {
        return resultCount;
    }

    public long getErrorCount()
    {
        return errorCount;
    }

    public long getResultSize()
    {
        return resultSize;
    }

    public long getMutationCount()
    {
        return mutationCount;
    }

    public long getWarningCount()
    {
        return warningCount;
    }
    public void setResultCount(int count)
    {
        resultCount = count;
    }
    public void setResultSize(int size)
    {
        resultSize = size;
    }
}
