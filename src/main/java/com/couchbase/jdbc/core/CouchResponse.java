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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by davec on 2015-06-23.
 */
public class CouchResponse
{
    AtomicBoolean fieldsInitialized = new AtomicBoolean(false);

    Map <String,String>    signature=null;
    ArrayList<Field> fields;

    List<CouchError> errors;
    List<CouchError> warnings;
    CouchMetrics metrics;
    String requestId;
    String status;
    List <Map<String,Object>> results;


    public CouchMetrics getMetrics()
    {
        return metrics;
    }


    // we don't know which will get called first so set the fields in getFields()

    public void setSignature(Map <String,String> signature)
    {
        this.signature = signature;
    }

    public ArrayList<Field>getFields()
    {
        // check to make sure we haven't set these yet
        if (!fieldsInitialized.getAndSet(true))
        {
            if (signature != null)
            {
                fields = new ArrayList<Field>(signature.size());
            }

            if (signature.containsKey("*")  )
            {
                if (metrics.getResultSize() > 0) {
                    Map<String,Object> firstRow = results.get(0);
                    Set <String>keySet = firstRow.keySet();

                    for (String key : keySet)
                    {
                        Object object =  firstRow.get(key);
                        if (object == null )
                        {
                            fields.add( new Field(key, "null"));
                        }
                        else
                        {
                            Object type = firstRow.get(key);
                            String jsonType="json";

                            if (type instanceof Number)
                                jsonType = "number";
                            else if ( type instanceof Boolean)
                                jsonType = "boolean";
                            else if ( type instanceof String)
                                jsonType = "string";
                            else if (type instanceof Map )
                                jsonType = "json";
                            else if ( type instanceof List )
                                jsonType = "json";

                            fields.add(new Field(key, jsonType));
                        }
                    }
                }
            }
            else
            {
                for (String key : signature.keySet())
                {
                    fields.add(new Field(key, signature.get(key)));
                }
            }
        }
        return fields;
    }
    public List <Map<String,Object>> getResults()
    {
        return results;
    }
    public void setResults(List results)
    {
        //noinspection unchecked
        this.results = results;
    }
    public void setMetrics(CouchMetrics metrics)
    {
        this.metrics = metrics;
    }
    public List <CouchError> getWarnings() {return warnings;}
    public List <CouchError> getErrors(){return errors;}

    public Map getFirstResult()
    {
        return (Map)results.get(0);
    }
}
