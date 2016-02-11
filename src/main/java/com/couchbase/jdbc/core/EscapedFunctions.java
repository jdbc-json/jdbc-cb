/*
 * //  Copyright (c) 2015 Couchbase, Inc.
 * //  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * //  except in compliance with the License. You may obtain a copy of the License at
 * //    http://www.apache.org/licenses/LICENSE-2.0
 * //  Unless required by applicable law or agreed to in writing, software distributed under the
 * //  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND * //  either express or implied. See the License for the specific language governing permissions
 * //  and limitations under the License.
 */

package com.couchbase.jdbc.core;

/**
 * Created by davec on 2015-09-18.
 */
/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;

public class EscapedFunctions
{
    // numeric functions names
    public final static String ABS="abs";
    public final static String ACOS="acos";
    public final static String ASIN="asin";
    public final static String ATAN="atan";
    public final static String ATAN2="atan2";
    public final static String CEILING="ceiling";
    public final static String COS="cos";
    public final static String COT="cot";
    public final static String DEGREES="degrees";
    public final static String EXP="exp";
    public final static String FLOOR="floor";
    public final static String LOG="log";
    public final static String LOG10="log10";
    public final static String MOD="mod";
    public final static String PI="pi";
    public final static String POWER="power";
    public final static String RADIANS="radians";
    public final static String ROUND="round";
    public final static String SIGN="sign";
    public final static String SIN="sin";
    public final static String SQRT="sqrt";
    public final static String TAN="tan";
    public final static String TRUNCATE="truncate";

    // string function names
    public final static String ASCII="ascii";
    public final static String CHAR="char";
    public final static String CONCAT="concat";
    public final static String INSERT="insert"; // change arguments order
    public final static String LCASE="lcase";
    public final static String LEFT="left";
    public final static String LENGTH="length";
    public final static String LOCATE="locate"; // the 3 args version duplicate args
    public final static String LTRIM="ltrim";
    public final static String REPEAT="repeat";
    public final static String REPLACE="replace";
    public final static String RIGHT="right"; // duplicate args
    public final static String RTRIM="rtrim";
    public final static String SPACE="space";
    public final static String SUBSTRING="substring";
    public final static String UCASE="ucase";
    // soundex is implemented on the server side by
    // the contrib/fuzzystrmatch module.  We provide a translation
    // for this in the driver, but since we don't want to bother with run
    // time detection of this module's installation we don't report this
    // method as supported in DatabaseMetaData.
    // difference is currently unsupported entirely.

    // date time function names
    public final static String CURDATE="curdate";
    public final static String CURTIME="curtime";
    public final static String DAYNAME="dayname";
    public final static String DAYOFMONTH="dayofmonth";
    public final static String DAYOFWEEK="dayofweek";
    public final static String DAYOFYEAR="dayofyear";
    public final static String HOUR="hour";
    public final static String MINUTE="minute";
    public final static String MONTH="month";
    public final static String MONTHNAME="monthname";
    public final static String NOW="clock_str";
    public final static String QUARTER="quarter";
    public final static String SECOND="second";
    public final static String WEEK="week";
    public final static String YEAR="year";
    // for timestampadd and timestampdiff the fractional part of second is not supported
    // by the backend
    // timestampdiff is very partially supported
    public final static String TIMESTAMPADD="timestampadd";
    public final static String TIMESTAMPDIFF="timestampdiff";

    // constants for timestampadd and timestampdiff
    public final static String SQL_TSI_ROOT="SQL_TSI_";
    public final static String SQL_TSI_DAY="DAY";
    public final static String SQL_TSI_FRAC_SECOND="FRAC_SECOND";
    public final static String SQL_TSI_HOUR="HOUR";
    public final static String SQL_TSI_MINUTE="MINUTE";
    public final static String SQL_TSI_MONTH="MONTH";
    public final static String SQL_TSI_QUARTER="QUARTER";
    public final static String SQL_TSI_SECOND="SECOND";
    public final static String SQL_TSI_WEEK="WEEK";
    public final static String SQL_TSI_YEAR="YEAR";


    // system functions
    public final static String DATABASE="database";
    public final static String IFNULL="ifnull";
    public final static String USER="user";


    /** storage for functions implementations */
    private static Map functionMap = createFunctionMap();

    private static Map createFunctionMap() {
        Method[] arrayMeths = EscapedFunctions.class.getDeclaredMethods();
        Map functionMap = new HashMap(arrayMeths.length*2);
        for (Method meth : arrayMeths) {
            if (meth.getName().startsWith("sql"))
                functionMap.put(meth.getName().toLowerCase(Locale.US), meth);
        }
        return functionMap;
    }

    /**
     * get Method object implementing the given function
     * @param functionName name of the searched function
     * @return a Method object or null if not found
     */
    public static Method getFunction(String functionName){
        return (Method) functionMap.get("sql"+functionName.toLowerCase(Locale.US));
    }

    // ** numeric functions translations **
    /* ceiling to ceil translation */
    public static String sqlceiling(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","ceiling"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("ceil(");
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();
    }

    /* log to ln translation */
    public static String sqllog(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","log"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("ln(");
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();
    }
    public static String sqlcot(List <StringBuilder> parsedArgs) throws SQLException
    {
        if (parsedArgs.size() != 1) {
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.", "cot"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("1/tan(");
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();

    }
    /* log10 to log translation */
    public static String sqllog10(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","log10"));
        }
        StringBuilder buf = new StringBuilder();
        buf.append("log(");
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();
    }

    /* power to pow translation */
    public static String sqlpower(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=2){
            throw new CouchBaseSQLException(String.format("{0} function takes two and only two arguments.","power"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("power(");
        buf.append(parsedArgs.get(0)).append(',').append(parsedArgs.get(1));
        return buf.append(')').toString();
    }

    /* truncate to trunc translation */
    public static String sqltruncate(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=2){
            throw new CouchBaseSQLException(String.format("{0} function takes two and only two arguments.","truncate"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("trunc(");
        buf.append(parsedArgs.get(0)).append(',').append(parsedArgs.get(1));
        return buf.append(')').toString();
    }

    // ** string functions translations **
    /* char to chr translation */
    public static String sqlchar(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","char"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("chr(");
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();
    }

    /* concat translation */
    public static String sqlconcat(List parsedArgs){
        StringBuilder buf = new StringBuilder();
        buf.append("concat(");
        for (int iArg = 0;iArg<parsedArgs.size();iArg++){
            buf.append(parsedArgs.get(iArg));
            if (iArg!=(parsedArgs.size()-1))
                buf.append(", ");
        }
        return buf.append(')').toString();
    }

    /* insert to overlay translation */
    public static String sqlinsert(List <StringBuilder> parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=4){
            throw new CouchBaseSQLException(String.format("{0} function takes four and only four argument.","insert"));
        }
        int startpluslength = Integer.parseInt(parsedArgs.get(1).toString()) + Integer.parseInt(parsedArgs.get(2).toString());

        StringBuilder buf = new StringBuilder();

        buf.append("concat(").
        // substring from string1(0,start)
        append("substr(").append(parsedArgs.get(0)).append(',').append(0).append(',').append(parsedArgs.get(1)).append("-1)").
        // substring from start + length
        append(',').append(parsedArgs.get(3)).append(',').
        append("substr(").append(parsedArgs.get(0)).append(',').append(startpluslength-1).append(')');

        return buf.append(')').toString();
    }

    /* lcase to lower translation */
    public static String sqllcase(List parsedArgs) throws SQLException
    {

        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","lcase"));
        }


        StringBuilder buf = new StringBuilder();
        buf.append("lower(");
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();
    }

    /* left to substring translation */
    public static String sqlleft(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=2){
            throw new CouchBaseSQLException(String.format("{0} function takes two and only two arguments.","left"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("substr(");
        buf.append(parsedArgs.get(0)).append(",0,").append(parsedArgs.get(1));
        return buf.append(')').toString();
    }

    /* length translation */
    public static String sqllength(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","length"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("length( rtrim(");
        buf.append(parsedArgs.get(0));
        return buf.append("))").toString();
    }

    /* locate translation */
    public static String sqllocate(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()==2){
            return "(position("+parsedArgs.get(1)+" , "+parsedArgs.get(0)+")+1)";
        }else if (parsedArgs.size()==3){
            return "(position("+parsedArgs.get(1)+", substr("+parsedArgs.get(0)+" , "+parsedArgs.get(2)+"))+1)";
        }else{
            throw new CouchBaseSQLException(String.format("{0} function takes two or three arguments.","locate"));
        }
    }

    /* ltrim translation */
    public static String sqlltrim(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","ltrim"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("ltrim( ");
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();
    }

    /* right to substring translation */
    public static String sqlright(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=2){
            throw new CouchBaseSQLException(String.format("{0} function takes two and only two arguments.","right"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("substr(");
        buf.append(parsedArgs.get(0)).append(',').append("-").append(parsedArgs.get(1));
        return buf.append(")").toString();
    }

    /* rtrim translation */
    public static String sqlrtrim(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","rtrim"));
        }

        StringBuilder buf = new StringBuilder();
        buf.append("rtrim( ");
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();
    }

    /* space translation */
    public static String sqlspace(List parsedArgs) throws SQLException
    {
        StringBuilder buf = new StringBuilder();
        buf.append("repeat(' ',");
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","space"));
        }
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();
    }

    /* substring to substr translation */
    public static String sqlsubstring(List parsedArgs) throws SQLException
    {
        if (parsedArgs.size()==2){
            return "substr("+parsedArgs.get(0)+","+parsedArgs.get(1)+"-1)";
        }else if (parsedArgs.size()==3){
            return "substr("+parsedArgs.get(0)+","+parsedArgs.get(1)+"-1,"+parsedArgs.get(2)+")";
        }else{
            throw new CouchBaseSQLException(String.format("{0} function takes two or three arguments.","substring"));
        }
    }

    /* ucase to upper translation */
    public static String sqlucase(List parsedArgs) throws SQLException{
        StringBuilder buf = new StringBuilder();
        buf.append("upper(");
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","ucase"));
        }
        buf.append(parsedArgs.get(0));
        return buf.append(')').toString();
    }
    public static String sqlnow(List parsedArgs) throws  SQLException {
        if (parsedArgs.size()!=0){
            throw new CouchBaseSQLException(String.format("{0} function doesn''t take any argument.","now"));
        }
        return "clock_str()";

    }
    /* curdate to current_date translation */
    public static String sqlcurdate(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=0){
            throw new CouchBaseSQLException(String.format("{0} function doesn''t take any argument.","curdate"));
        }
        return "clock_str('2006-01-01')";
    }

    /* curtime to current_time translation */
    public static String sqlcurtime(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=0){
            throw new CouchBaseSQLException(String.format("{0} function doesn''t take any argument.","curtime"));
        }
        return "clock_str('15:05:05')";
    }

    //TODO replace this with correct function when it becomes available
    /* dayname translation */
    public static String sqldayname(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","dayname"));
        }
        return "(case date_part_str("+parsedArgs.get(0)+",'dow') when 0 then 'SUN' WHEN 1 THEN 'MON' WHEN 2 THEN 'TUE' WHEN 3 THEN 'WED' WHEN 4 THEN 'THU' WHEN 5 THEN 'FRI' WHEN 6 THEN 'SAT' END)";
    }

    /* dayofmonth translation */
    public static String sqldayofmonth(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","dayofmonth"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'day')";
    }

    /* dayofweek translation
     * adding 1 to  function since we expect values from 1 to 7 */
    public static String sqldayofweek(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","dayofweek"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'iso_dow') ";
    }

    /* dayofyear translation */
    public static String sqldayofyear(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","dayofyear"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'doy')";
    }

    /* hour translation */
    public static String sqlhour(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","hour"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'hour')";
    }

    /* minute translation */
    public static String sqlminute(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","minute"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'minute')";
    }

    /* month translation */
    public static String sqlmonth(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","month"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'month')";
    }

    //TODO replace this with correct function when it becomes available
    /* monthname translation */
    public static String sqlmonthname(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","monthname"));
        }
        return "(case date_part_str("+parsedArgs.get(0)+",'month') WHEN 1 THEN 'JAN' WHEN 2 THEN 'FEB' WHEN 3 THEN 'MAR' WHEN 4 THEN 'APR' WHEN 5 THEN 'MAY' WHEN 6 THEN 'JUN'" +
                " WHEN 7 THEN 'JUL' WHEN 8 THEN 'AUG' WHEN 9 THEN 'SEP' WHEN 10 THEN 'OCT' WHEN 11 THEN 'NOV' WHEN 12 THEN 'DEC' END)";

    }

    /* quarter translation */
    public static String sqlquarter(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","quarter"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'quarter')";
    }

    /* second translation */
    public static String sqlsecond(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","second"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'second')";
    }

    /* week translation */
    public static String sqlweek(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","week"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'week')";
    }

    /* year translation */
    public static String sqlyear(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=1){
            throw new CouchBaseSQLException(String.format("{0} function takes one and only one argument.","year"));
        }
        return "date_part_str("+parsedArgs.get(0)+",'year')";
    }

    /* time stamp add */
    public static String sqltimestampadd(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=3){
            throw new CouchBaseSQLException(String.format("{0} function takes three and only three arguments.","timestampadd"));
        }
        String interval = EscapedFunctions.constantToInterval(parsedArgs.get(0).toString());
        StringBuilder buf = new StringBuilder("DATE_ADD_STR(");
        buf.append(parsedArgs.get(2)).append(',').append(parsedArgs.get(1)).append(',');
        buf.append(interval).append(")");
        return buf.toString();
    }

    private final static String constantToInterval(String type)throws SQLException{
        if (!type.startsWith(SQL_TSI_ROOT))
            throw new CouchBaseSQLException(String.format("Interval {0} not yet implemented",type));
        String shortType = type.substring(SQL_TSI_ROOT.length());
        if (SQL_TSI_DAY.equalsIgnoreCase(shortType))
            return "'day'";
        else if (SQL_TSI_SECOND.equalsIgnoreCase(shortType))
            return "'second'";
        else if (SQL_TSI_HOUR.equalsIgnoreCase(shortType))
            return "'hour'";
        else if (SQL_TSI_MINUTE.equalsIgnoreCase(shortType))
            return "'minute'";
        else if (SQL_TSI_MONTH.equalsIgnoreCase(shortType))
            return "'month'";
        else if (SQL_TSI_QUARTER.equalsIgnoreCase(shortType))
            return "'quarter'";
        else if (SQL_TSI_WEEK.equalsIgnoreCase(shortType))
            return "'week'";
        else if (SQL_TSI_YEAR.equalsIgnoreCase(shortType))
            return "'year'";
        else if (SQL_TSI_FRAC_SECOND.equalsIgnoreCase(shortType))
            throw new CouchBaseSQLException(String.format("Interval {0} not yet implemented","SQL_TSI_FRAC_SECOND"));
        else throw new CouchBaseSQLException(String.format("Interval {0} not yet implemented",type));
    }


    /* time stamp diff */
    public static String sqltimestampdiff(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=3){
            throw new CouchBaseSQLException(String.format("{0} function takes three and only three arguments.","timestampdiff"));
        }
        String datePart = EscapedFunctions.constantToDatePart(parsedArgs.get(0).toString());
        StringBuilder buf = new StringBuilder();
        buf.append("date_diff_str( ").append(parsedArgs.get(2))
                .append(',').append(parsedArgs.get(1)).append(',').append(datePart).append(")");
        return buf.toString();
    }

    private final static String constantToDatePart(String type)throws SQLException{
        if (!type.startsWith(SQL_TSI_ROOT))
            throw new CouchBaseSQLException(String.format("Interval {0} not yet implemented",type));
        String shortType = type.substring(SQL_TSI_ROOT.length());
        if (SQL_TSI_DAY.equalsIgnoreCase(shortType))
            return "'day'";
        else if (SQL_TSI_SECOND.equalsIgnoreCase(shortType))
            return "'second'";
        else if (SQL_TSI_HOUR.equalsIgnoreCase(shortType))
            return "'hour'";
        else if (SQL_TSI_MINUTE.equalsIgnoreCase(shortType))
            return "'minute'";
            // See http://archives.postgresql.org/pgsql-jdbc/2006-03/msg00096.php
        else if (SQL_TSI_MONTH.equalsIgnoreCase(shortType))
            return "'month'";
        else if (SQL_TSI_QUARTER.equalsIgnoreCase(shortType))
            return "'quarter'";
        else if (SQL_TSI_WEEK.equalsIgnoreCase(shortType))
            return "'week'";
        else if (SQL_TSI_YEAR.equalsIgnoreCase(shortType))
            return "'year'";
        else if (SQL_TSI_FRAC_SECOND.equalsIgnoreCase(shortType))
            throw new CouchBaseSQLException(String.format("Interval {0} not yet implemented","SQL_TSI_FRAC_SECOND"));
        else throw new CouchBaseSQLException(String.format("Interval {0} not yet implemented",type));
    }

    /* database translation */
    public static String sqldatabase(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=0){
            throw new CouchBaseSQLException(String.format("{0} function doesn''t take any argument.","database"));
        }
        return "current_database()";
    }

    /* ifnull translation */
    public static String sqlifnull(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=2){
            throw new CouchBaseSQLException(String.format("{0} function takes two and only two arguments.","ifnull"));
        }
        return "coalesce("+parsedArgs.get(0)+","+parsedArgs.get(1)+")";
    }

    /* user translation */
    public static String sqluser(List parsedArgs) throws SQLException{
        if (parsedArgs.size()!=0){
            throw new CouchBaseSQLException(String.format("{0} function doesn''t take any argument.","user"));
        }
        return "user";
    }
}
