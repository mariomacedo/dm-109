package com.inatel.demos;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class TrabalhoFinalMarioEdson {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer");

    DataStream stream = env.addSource(
            new FlinkKafkaConsumer09<>("flink-demo", 
            new JSONDeserializationSchema(), 
            properties)
    );
 

    stream  .flatMap(new TelemetryJsonParser())
            .keyBy(0)
            .timeWindow(Time.seconds(3))
            .reduce(new GearReducer())
            .flatMap(new GearMapper())
            .map(new GearPrinter())
            .print();

    env.execute();
    }

    static class TelemetryJsonParser implements FlatMapFunction<ObjectNode, Tuple3<String, Float, Integer>> {
      @Override
      public void flatMap(ObjectNode jsonTelemetry, Collector<Tuple3<String, Float, Integer>> out) throws Exception {
        String carNumber = "car" + jsonTelemetry.get("Car").asText();
        float gear = jsonTelemetry.get("telemetry").get("Gear").floatValue();
        int n = jsonTelemetry.get("Car").asInt();
        // Carro Escolhido car4
        if(n == 4) {
          out.collect(new Tuple3<>(carNumber,  gear, 0 ));
        }        
      }
    }

    static class GearReducer implements ReduceFunction<Tuple3<String, Float, Integer>> {
      @Override
      public Tuple3<String, Float, Integer> reduce(Tuple3<String, Float,Integer> value1, Tuple3<String, Float, Integer> value2) {
        return new Tuple3<>(value1.f0, value2.f1, (value2.f1 - value1.f1) == 0f ? value1.f2 : value1.f2+1 );
      }
    }


    static class GearMapper implements FlatMapFunction<Tuple3<String, Float, Integer>, Tuple2<String, Integer>> {
      @Override
      public void flatMap(Tuple3<String, Float, Integer> carInfo, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(  new Tuple2<>( carInfo.f0 , carInfo.f2 )  );
      }
    }
  
    static class GearPrinter implements MapFunction<Tuple2<String, Integer>, String> {
      @Override
      public String map(Tuple2<String, Integer> avgEntry) throws Exception {
        return  String.format("Car %s : %d ", avgEntry.f0 , avgEntry.f1 ) ;
      }
    }
  }