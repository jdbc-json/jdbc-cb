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

import javax.json.JsonValue;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by davec on 2015-06-23.
 */
public class CouchResponse
{
    String    clientContextID;
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

    // there is only one so create the field now
    // TODO need testcase
    public void setSignature(String signature)
    {
        fields = new ArrayList<Field>(1);
        fields.add(new Field(signature, JsonValue.ValueType.STRING.name()));
        fieldsInitialized.set(true);
    }

    // we don't know which will get called first so set the fields in getFields()

    public void setSignature(Map <String,String> signature)
    {
        this.signature = signature;
    }

    public ArrayList<Field>getFields()
    {
        // check to make sure we haven't set these yet
        if (fieldsInitialized.getAndSet(true) == false)
        {
            if (signature != null)
            {
                fields = new ArrayList<Field>(signature.size());
            }

            if (signature.containsKey("*"))
            {
                if (metrics.getResultSize() > 0) {
                    Map<String,Object> firstRow = (Map<String,Object>)results.get(0);
                    Set <String>keySet = firstRow.keySet();

                    //TODO this is wrong FIXME
                    for (String key : keySet) {
                        fields.add(new Field(key, firstRow.get(key).toString()));
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
    public List <CouchError> getWarnings() {return warnings;}
    public List <CouchError> getErrors(){return errors;}

}
