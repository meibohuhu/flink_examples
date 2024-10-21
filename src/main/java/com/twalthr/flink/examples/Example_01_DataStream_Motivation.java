package com.twalthr.flink.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Basic example of generating data and printing it.
 *  Creates a DataStream from a set of predefined element */
public class Example_01_DataStream_Motivation {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.fromElements(ExampleData.CUSTOMERS)
        .filter(new FilterFunction<Customer>() {
            @Override
            public boolean filter(Customer customer) {
                // Define the filter condition: only keep customers older than 30
                return !customer.c_name.equals("Alice"); // Change the condition as needed
            }
        })
        .executeAndCollect()
        .forEachRemaining(System.out::println);
  }
}
