package com.stream_processing;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import org.apache.spark.sql.streaming.*;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

public final class Stream_cal_final_result implements Serializable {
    public static void main(String[] args) {
        Aggregate_result result = new Aggregate_result();
        try {
            result.main();
        } catch (TimeoutException | StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}

class Aggregate_result implements Serializable {
    private SparkSession spark;
    private Dataset<Row> df;
    private String conn_url;
    private Kafka_df kdf_obj;
    
    public Aggregate_result() {
        this.spark = SparkSession.builder()
                        .appName("Stream_aggregation")
                        .master("local[4]")
                        .config("spark.executor.memory", "4g")
                        .config("spark.streaming.concurrentJobs", "5")   
                        .getOrCreate();

        this.kdf_obj = new Kafka_df();
        this.conn_url = "jdbc:postgresql://postgres:5432/postgres?user=postgres&password=password";
    }

    public void main() throws TimeoutException, StreamingQueryException {        
        this.df = kdf_obj.Read_from_kafka(this.spark, "checkouts");

        this.df = kdf_obj.Explode_kafka_to_df(this.df, "checkouts",
                            new String[] {
                                "product_id", "user_id", "purchase_id", "total_cost", "payment_method",
                                "quantity",  "payment_status", "description", "purchase_time", 
                                "name", "email", "gender", "birth_day", "location", "phone",
                                "registration_date", "product_name", "category", "brand", 
                                "profit", "click_id", "click_time", "click_at", "source", "ip"
                            }    
                        );
        
        this.df = kdf_obj.Change_columns_type(this.df, DataTypes.DoubleType, 
                            new String[] {"quantity", "profit", "total_cost"}
                        );
        
        this.df = kdf_obj.Change_columns_type(this.df, DataTypes.TimestampType, 
                            new String[] {"click_time", "purchase_time"}
                        );

        this.df = kdf_obj.Change_columns_type(this.df, DataTypes.DateType, 
                            new String[] {"registration_date", "birth_day"}
                        );

        
        this.df = this.df.withWatermark("purchase_time", "1 minute");

        Write_result_to_sink("source", "success", "source_count");
        Write_result_to_sink("description", "failure", "fail_count");
        Write_result_to_sink("category", "success", "category_count");  
        Write_result_to_sink("gender", "success", "gender_count");
        Write_result_to_sink("revenue", "success", "revenue");
        
        this.spark.streams().awaitAnyTermination();

        this.spark.close();
    }

    private void Write_result_to_sink(String col, String type, String table) throws TimeoutException, StreamingQueryException { 
        Dataset<Row> final_df = this.df.where(functions.col("payment_status").equalTo(type));
                                       
        if (col == "revenue")
            final_df = final_df.select("total_cost", "profit", "quantity", "purchase_time")
                                .groupBy(functions.window(functions.col("purchase_time"), "1 minute"))
                                .agg(functions.sum("total_cost").alias("revenue"), 
                                    functions.sum("profit").alias("profit"),
                                    functions.sum("quantity").alias("quantity"));
        else {
            final_df = final_df.select(col, "purchase_time")
                                .groupBy(functions.col(col), functions.window(functions.col("purchase_time"), "1 minute"))
                                .count();
        }

        final_df = final_df.withColumn("time", functions.col("window").getField("end")).drop("window");

        StreamingQuery query = final_df.writeStream()
                                        .foreachBatch((batchDF, batchId) -> {
                                            batchDF.write()
                                                .format("jdbc")
                                                .option("url", this.conn_url)
                                                .option("dbtable", "result." + table)
                                                .mode("Append")
                                                .save();
                                        })
                                        .trigger(Trigger.ProcessingTime("5 seconds"))
                                        .outputMode("append")
                                        .start();
    }
}