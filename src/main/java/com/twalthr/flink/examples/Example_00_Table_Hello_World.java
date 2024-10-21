package com.twalthr.flink.examples;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/** Hello world example. */
public class Example_00_Table_Hello_World {

  public static void main(String[] args) {

    System.out.print("hhahah");
    TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

//    testCreateTable(env);

    // Fully programmatic, Purpose: Creates a table from a single value 1, executes the query, and prints the result.
    env.fromValues(1).execute().print();

    // Flink SQL is included: Executes a simple SQL query (SELECT 1) and prints the result.
    env.sqlQuery("SELECT 1").execute().print();

    // The API feels like SQL (built-in functions, data types, ...)
    Table table = env.fromValues("1").as("c");
//    // Everything is centered around Table objects, conceptually SQL views (=virtual tables)
    env.createTemporaryView("InputTable", table);
    env.sqlQuery("SELECT * FROM InputTable").execute().print();

//    Inserts data from the InputTable view into the blackhole connector. The blackhole connector discards all incoming data.
    env.from("InputTable").insertInto(TableDescriptor.forConnector("blackhole").build()).execute();
    env.sqlQuery("SELECT * FROM InputTable").execute().print();

//    // Let's get started with unbounded data...
//    env.from(
//            TableDescriptor.forConnector("datagen")
//                .schema(
//                    Schema.newBuilder()
//                        .column("uid", DataTypes.BIGINT())
//                        .column("s", DataTypes.STRING())
//                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
//                        .build())
//                .build())
//        .execute()
//        .print();
  }

  public static void testCreateTable(TableEnvironment env) {
    // Register a temporary table from values
    env.executeSql(
            "CREATE TEMPORARY VIEW MyTable AS " +
                    "SELECT * " +
                    "FROM (VALUES (1, 13, 'John'), (2, 12, 'Alice'), (3, 42, 'Bob')) AS T(id, age, name)"
    );
    // Run an SQL query
    env.executeSql("SELECT name, age FROM MyTable where id = 2").print();
  }
}
