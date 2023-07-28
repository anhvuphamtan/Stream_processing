package com.stream_processing;

import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;

public class Kafka_df {
    private String bootstrap_server;

    public Kafka_df() {
        this.bootstrap_server = "kafka:9092";
    }

    public String Get_bootstrap_server() {
        return this.bootstrap_server;
    }

    public Dataset<Row> Read_from_kafka(SparkSession spark, String topics) { 
        return spark.readStream()
                    .format("kafka")
                    .option("failOnDataLoss", false)
                    .option("kafka.bootstrap.servers", this.bootstrap_server)                          
                    .option("subscribe", topics)
                    .load();
    }
    
    public StreamingQuery Write_to_kafka(Dataset<Row> df, String topic, String mode, String checkpoint_location) 
        throws TimeoutException { 
            return df.select(functions.to_json(functions.struct("*")).alias("value"))
                                        .writeStream()
                                        .format("kafka")
                                        .outputMode(mode)
                                        .option("kafka.bootstrap.servers", this.bootstrap_server)
                                        .option("topic", topic)
                                        .option("checkpointLocation", checkpoint_location)
                                        .start();
    }

    public Dataset<Row> Retrieve_data(Dataset<Row> df, String topic) {
        df = df.filter(df.col("topic").equalTo(topic));
        return df.selectExpr("CAST(VALUE AS STRING)");
    }

    public StructType Create_schema(String[] col_list) {
        List<StructField> fields = new ArrayList<>();
        
        for (String col : col_list) {
            fields.add(DataTypes.createStructField(col, DataTypes.StringType, false));
        }

        StructType schema = DataTypes.createStructType(fields);

        return schema;
    }

    public Dataset<Row> Explode_kafka_to_df(Dataset<Row> df, String topic, String[] col_list) {
        df = Retrieve_data(df, topic);
        StructType schema = Create_schema(col_list);

        df = df.withColumn("VALUE", functions.from_json(functions.col("VALUE"), schema).alias("VALUE"))
                .selectExpr("VALUE.*");

        for (int i = 0; i < df.columns().length; i++) {
            String col_name = df.columns()[i];
            df = df.withColumnRenamed(col_name, col_name.toLowerCase());
        }

        return df;
    }

    public Dataset<Row> Change_columns_type(Dataset<Row> df, DataType type, String[] columns) {
        for (String column : columns) {
            df = df.withColumn(column, functions.col(column).cast(type));
        }

        return df;
    }
}
