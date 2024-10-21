package com.twalthr.flink.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.Period;

/** Use arbitrary libraries to perform streaming ETL i.e. convert records into JSON. */
/*
/*
ObjectMapper is used primarily for converting between Java objects and JSON strings, as well as for creating complex JSON structures. It operates at a higher abstraction level, allowing easy conversion of entire objects.
JsonObject is used to work with JSON data directly, providing methods to manipulate JSON structures without the need for Java objects.
 */
public class Example_03_DataStream_ETL {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.fromElements(ExampleData.CUSTOMERS)
        // use process function to access streaming building blocks
        .process(
            new ProcessFunction<Customer, String>() {
              @Override
              public void processElement(
                  Customer customer,
                  ProcessFunction<Customer, String>.Context context,
                  Collector<String> out)
                  throws JsonProcessingException {
                ObjectMapper objectMapper = new ObjectMapper();
                ObjectNode json = objectMapper.createObjectNode();
                json.put("name", customer.c_name);
                json.put("age", Period.between(customer.c_birthday, LocalDate.now()).getYears());
                String output = objectMapper.writeValueAsString(json);
                out.collect(output);
              }
            })
        .executeAndCollect()
        .forEachRemaining(System.out::println);
  }
}
