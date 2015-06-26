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

package com.couchbase.json;

import java.io.InputStream;
import java.io.Reader;
import java.util.Map;

/**
 * Created by davec on 2015-06-26.
 */
public interface SQLJSON
{
    public void free();
    public InputStream getBinaryStream();
    public Reader getCharacterStream();
    public String getString();
    public void setString();
    public Object parse(Class clazz);
    public Map parse();
}
