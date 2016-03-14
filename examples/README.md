# How to Use the Driver

This directory contains example files that show how to use the driver. 
All five files have `main()` functions, so you can run them directly;
they are set up to connect to the Couchbase instance running on the local machine.

The first four require *beer-sample* and *default* buckets to exist and have primary indexes. 
The last one has more specific set-up instructions in the comments of the file itself.

* *SimpleSelects.java* shows how to retrieve simple JSON values -- strings, numbers, and booleans -- from Couchbase.
* *CrudOperations.java* shows how to create, read, update, and delete entries in Couchbase using the N1QL query languge,
through the driver.
* *PreparedStatements.java* shows how to use prepared statements and parameters. There queries are readied once, and can then
be executed multiple times with varying parameters.
* *CompoundElements.java* shows how to insert and retrieve JSON object and array values.
* In Couchbase, buckets can be protected with passwords. *UseCredentials.java* shows how to provide such passwords to the driver
to allow access to these buckets.
