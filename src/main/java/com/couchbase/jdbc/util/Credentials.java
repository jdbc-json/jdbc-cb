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

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by davec on 2015-05-12.
 */
public class Credentials
{

    List <Credential> credentials = new ArrayList<Credential>();

    public  Credentials()
    {
    }

    public Credentials add(String user, String password)
    {
        this.credentials.add(new Credential(user,password));
        return this;
    }

    private class Credential
    {
        String user, password;

        Credential(String user, String password)
        {
            this.user       = user;
            this.password   = password;
        }

    }
    public String toString()
    {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        JsonObjectBuilder objectBuilder;

        for (Credential credential : credentials)
        {
            objectBuilder = Json.createObjectBuilder();
            arrayBuilder.add(objectBuilder.add("user",credential.user).add("pass", credential.password).build());
        }
        return arrayBuilder.build().toString();
    }
}
