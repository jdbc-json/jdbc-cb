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

import com.couchbase.jdbc.CouchBaseTestCase;
import com.couchbase.jdbc.util.SqlParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.*;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * Created by davec on 2015-09-18.
 */
@RunWith(JUnit4.class)
public class SqlParserTest extends CouchBaseTestCase
{

    @Test
    public void testReplaceProcessing() throws Exception
    {
        SqlParser sqlParser = new SqlParser("select {fn acos(-0.6)} as acos ");
        String replaced = sqlParser.replaceProcessing("select {fn acos(-0.6)} as acos",true);
        assertEquals("select acos(-0.6) as acos", replaced);

        replaced  = sqlParser.replaceProcessing("insert into escapetest (ts) values ({ts '1900-01-01 00:00:00'})", true);
        assertEquals("insert into escapetest (ts) values ( '1900-01-01 00:00:00')", replaced);

        replaced = sqlParser.replaceProcessing("insert into escapetest (d) values ({d '1900-01-01'})",true);
        assertEquals("insert into escapetest (d) values (DATE  '1900-01-01')", replaced);

        replaced = sqlParser.replaceProcessing("insert into escapetest (t) values ({t '00:00:00'})",true);
        assertEquals("insert into escapetest (t) values ( '00:00:00')", replaced);

        replaced = sqlParser.replaceProcessing("select {fn version()} as version", true);
        assertEquals("select version() as version",replaced);

        replaced = sqlParser.replaceProcessing("select {fn version()} as version, {fn log({fn log(3.0)})} as log", true);
        assertEquals("select version() as version, ln(ln(3.0)) as log",replaced);

        replaced = sqlParser.replaceProcessing("select * from {oj test_statement a left outer join b on (a.i=b.i)} ", true);
        assertEquals("select * from  test_statement a left outer join b on (a.i=b.i) ",replaced);

    }

    @Test
    public void testNumericFunctions() throws Exception
    {
        Statement stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("select {fn abs(-2.3)} as abs ");
        assertTrue(rs.next());
        assertEquals(2.3f, rs.getFloat(1), 0.00001);

        rs = stmt.executeQuery("select {fn acos(-0.6)} as acos ");
        assertTrue(rs.next());
        assertEquals(Math.acos(-0.6), rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn asin(-0.6)} as asin ");
        assertTrue(rs.next());
        assertEquals(Math.asin(-0.6), rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn atan(-0.6)} as atan ");
        assertTrue(rs.next());
        assertEquals(Math.atan(-0.6), rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn atan2(-2.3,7)} as atan2 ");
        assertTrue(rs.next());
        assertEquals(Math.atan2(-2.3,7), rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn ceiling(-2.3)} as ceiling ");
        assertTrue(rs.next());
        assertEquals(-2, rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn cos(-2.3)} as cos, {fn cot(-2.3)} as cot ");
        assertTrue(rs.next());
        assertEquals(Math.cos(-2.3), rs.getDouble(1), 0.00001);
        assertEquals(1/Math.tan(-2.3), rs.getDouble(2), 0.00001);

        rs = stmt.executeQuery("select {fn degrees({fn pi()})} as degrees ");
        assertTrue(rs.next());
        assertEquals(180, rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn exp(-2.3)}, {fn floor(-2.3)}," +
            " {fn log(2.3)},{fn log10(2.3)},{fn mod(3,2)}");
        assertTrue(rs.next());
        assertEquals(Math.exp(-2.3), rs.getDouble(1), 0.00001);
        assertEquals(-3, rs.getDouble(2), 0.00001);
        assertEquals(Math.log(2.3), rs.getDouble(3), 0.00001);
        assertEquals(Math.log(2.3)/Math.log(10), rs.getDouble(4), 0.00001);
        assertEquals(1, rs.getDouble(5), 0.00001);

        rs = stmt.executeQuery("select {fn pi()}, {fn power(7,-2.3)}," +
            " {fn radians(-180)},{fn round(3.1294,2)}");
        assertTrue(rs.next());
        assertEquals(Math.PI, rs.getDouble(1), 0.00001);
        assertEquals(Math.pow(7,-2.3), rs.getDouble(2), 0.00001);
        assertEquals(-Math.PI, rs.getDouble(3), 0.00001);
        assertEquals(3.13, rs.getDouble(4), 0.00001);

        rs = stmt.executeQuery("select {fn sign(-2.3)}, {fn sin(-2.3)}," +
            " {fn sqrt(2.3)},{fn tan(-2.3)},{fn truncate(3.1294,2)}");
        assertTrue(rs.next());
        assertEquals(-1, rs.getInt(1));
        assertEquals(Math.sin(-2.3), rs.getDouble(2), 0.00001);
        assertEquals(Math.sqrt(2.3), rs.getDouble(3), 0.00001);
        assertEquals(Math.tan(-2.3), rs.getDouble(4), 0.00001);
        assertEquals(3.12, rs.getDouble(5), 0.00001);
    }

    @Test
    public void testStringFunctions() throws SQLException
    {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select {fn concat('ab','cd')}" +
                ",{fn lcase('aBcD')},{fn left('1234',2)},{fn length('123 ')}" +
                ",{fn locate('bc','abc')},{fn locate('bc','abc',3)}");
        assertTrue(rs.next());
        assertEquals("abcd",rs.getString(1));
        assertEquals("abcd",rs.getString(2));
        assertEquals("12",rs.getString(3));
        assertEquals(3,rs.getInt(4));
        assertEquals(2,rs.getInt(5));
        assertEquals(0,rs.getInt(6));

        rs = stmt.executeQuery("SELECT {fn insert('abcdef',3,2,'xxxx')}" +
                ",{fn replace('abcdbc','bc','x')}");
        assertTrue(rs.next());
        assertEquals("abxxxxef",rs.getString(1));
        assertEquals("axdx",rs.getString(2));

        rs = stmt.executeQuery("select {fn ltrim(' ab')},{fn repeat('ab',2)}" +
                ",{fn right('abcde',2)},{fn rtrim('ab ')}" +
                ",{fn space(3)},{fn substring('abcd',2,2)}" +
                ",{fn ucase('aBcD')}");
        assertTrue(rs.next());
        assertEquals("ab",rs.getString(1));
        assertEquals("abab",rs.getString(2));
        assertEquals("de",rs.getString(3));
        assertEquals("ab",rs.getString(4));
        assertEquals("   ",rs.getString(5));
        assertEquals("bc",rs.getString(6));
        assertEquals("ABCD",rs.getString(7));
    }

    @Test
    public void testDateFunctions() throws SQLException
    {
        Calendar expected = Calendar.getInstance(), actual=Calendar.getInstance();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select {fn curdate()} as curdate,{fn curtime()} as curtime" +
                ",{fn dayname('2015-02-22 12:00:00')} as dayname , {fn dayofmonth('2015-02-22 12:00:00')} as dayofmonth" +
                ",{fn dayofweek({ts '2015-02-22 12:00:00'})} as dayofweek,{fn dayofyear('2015-02-22 12:00:00')} as dayofyear" +
                ",{fn hour('2015-02-22 12:00:00')} as hour,{fn minute('2015-02-22 12:00:00')} as minute" +
                ",{fn month('2015-02-22 12:00:00')} as month" +
                ",{fn monthname('2015-02-22 12:00:00')} as monthname,{fn quarter('2015-02-22 12:00:00')} as quarter" +
                ",{fn second('2015-02-22 12:00:00')} as second,{fn week('2015-02-22 12:00:00')} as week" +
                ",{fn year('2015-02-22 12:00:00')} as year");
        assertTrue(rs.next());

        Date date = rs.getDate("curdate");
        actual.setTime(date);

        assertEquals(expected.get(Calendar.DAY_OF_MONTH),actual.get(Calendar.DAY_OF_MONTH));
        assertEquals(expected.get(Calendar.YEAR), actual.get(Calendar.YEAR));
        assertEquals(expected.get(Calendar.MONTH),actual.get(Calendar.MONTH));

        Time time = rs.getTime("curtime");
        assertNotNull(time);

        String dayName = rs.getString("dayname");
        assertEquals("SUN", dayName);

        int dayOfMonth = rs.getInt("dayofmonth");
        assertEquals(22, dayOfMonth);

        int dayOfWeek = rs.getInt("dayofweek");
        assertEquals(7,dayOfWeek);

        int dayOfYear = rs.getInt("dayofyear");
        assertEquals(53, dayOfYear);

        int hour = rs.getInt("hour");
        assertEquals(12,hour);

        int minute = rs.getInt("minute");
        assertEquals(0,minute);

        int month = rs.getInt("month");
        assertEquals(2,month);

        String monthName = rs.getString("monthname");
        assertEquals("FEB", monthName);

        int quarter = rs.getInt("quarter");
        assertEquals(1,quarter);

        int second = rs.getInt("second");
        assertEquals(0,second);

        int week = rs.getInt("week");
        assertEquals(8, week);

        int year = rs.getInt("year");
        assertEquals(2015,year);

        rs = stmt.executeQuery("select {fn timestampadd(SQL_TSI_SECOND,30, '2015-02-22 12:00:00'");
        assertTrue(rs.next());
        Timestamp ts= rs.getTimestamp(1);

        actual.setTime(ts);
        assertEquals(30, actual.get(Calendar.SECOND));

        rs = stmt.executeQuery("select {fn timestampadd(SQL_TSI_MINUTE,25, '2015-02-22 12:00:00'");
        assertTrue(rs.next());
        ts= rs.getTimestamp(1);

        actual.setTime(ts);
        assertEquals(25, actual.get(Calendar.MINUTE));

        rs = stmt.executeQuery("select {fn timestampadd(SQL_TSI_HOUR,1, '2015-02-22 12:00:00'");
        assertTrue(rs.next());
        ts= rs.getTimestamp(1);

        actual.setTime(ts);
        assertEquals(13, actual.get(Calendar.HOUR_OF_DAY));

        rs = stmt.executeQuery("select {fn timestampadd(SQL_TSI_DAY,1, '2015-02-22 12:00:00'");
        assertTrue(rs.next());
        ts= rs.getTimestamp(1);

        actual.setTime(ts);
        assertEquals(23, actual.get(Calendar.DAY_OF_MONTH));

        rs = stmt.executeQuery("select {fn timestampadd(SQL_TSI_WEEK,1, '2015-02-22 12:00:00'");
        assertTrue(rs.next());
        ts= rs.getTimestamp(1);

        actual.setTime(ts);
        assertEquals(10, actual.get(Calendar.WEEK_OF_YEAR));

        rs = stmt.executeQuery("select {fn timestampadd(SQL_TSI_MONTH,1, '2015-02-22 12:00:00'");
        assertTrue(rs.next());
        ts= rs.getTimestamp(1);

        actual.setTime(ts);
        assertEquals(2, actual.get(Calendar.MONTH));


        // second
        rs = stmt.executeQuery("select {fn timestampdiff(SQL_TSI_SECOND,{fn now()},{fn timestampadd(SQL_TSI_SECOND,3,{fn now()})})} ");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        //      MINUTE
        rs = stmt.executeQuery("select {fn timestampdiff(SQL_TSI_MINUTE,{fn now()},{fn timestampadd(SQL_TSI_MINUTE,3,{fn now()})})} ");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        //      HOUR
        rs = stmt.executeQuery("select {fn timestampdiff(SQL_TSI_HOUR,{fn now()},{fn timestampadd(SQL_TSI_HOUR,3,{fn now()})})} ");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        //      day
        rs = stmt.executeQuery("select {fn timestampdiff(SQL_TSI_DAY,{fn now()},{fn timestampadd(SQL_TSI_DAY,-3,{fn now()})})} ");
        assertTrue(rs.next());
        assertEquals(-3,rs.getInt(1));
        //      WEEK => extract week from interval is not supported by backend
        rs = stmt.executeQuery("select {fn timestampdiff(SQL_TSI_WEEK,{fn now()},{fn timestampadd(SQL_TSI_WEEK,3,{fn now()})})} ");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
        //      YEAR
        rs = stmt.executeQuery("select {fn timestampdiff(SQL_TSI_YEAR,{fn now()},{fn timestampadd(SQL_TSI_YEAR,3,{fn now()})})} ");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));

        //      QUARTER => backend assume there are 1 quarter even in 270 days...
        rs = stmt.executeQuery("select {fn timestampdiff(SQL_TSI_QUARTER,{fn now()},{fn timestampadd(SQL_TSI_QUARTER,3,{fn now()})})} ");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));

        //      MONTH => backend assume there are 0 month in an interval of 92 days...
        rs = stmt.executeQuery("select {fn timestampdiff(SQL_TSI_MONTH,{fn now()},{fn timestampadd(SQL_TSI_MONTH,3,{fn now()})})} ");
        assertTrue(rs.next());
        assertEquals(3,rs.getInt(1));
    }

}