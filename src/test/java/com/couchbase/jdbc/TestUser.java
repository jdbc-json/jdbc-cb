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

/**
 * Created by davec on 2015-07-22.
 */
public class TestUser
{
    public String first;
    public String last;
    public int age;
    public boolean active;

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestUser testUser = (TestUser) o;

        if (age != testUser.age) return false;
        if (active != testUser.active) return false;
        if (first != null ? !first.equals(testUser.first) : testUser.first != null) return false;
        return !(last != null ? !last.equals(testUser.last) : testUser.last != null);

    }

    @Override
    public int hashCode()
    {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (last != null ? last.hashCode() : 0);
        result = 31 * result + age;
        result = 31 * result + (active ? 1 : 0);
        return result;
    }

    public TestUser(String first, String last, int age, boolean active)
    {
        this.first = first;
        this.last  = last;
        this.age   = age;
        this.active= active;

    }
    public String getFirst()
    {
        return first;
    }

    public void setFirst(String first)
    {
        this.first = first;
    }

    public String getLast()
    {
        return last;
    }

    public void setLast(String last)
    {
        this.last = last;
    }

    public int getAge()
    {
        return age;
    }

    public void setAge(int age)
    {
        this.age = age;
    }

    public boolean isActive()
    {
        return active;
    }

    public void setActive(boolean active)
    {
        this.active = active;
    }
}
