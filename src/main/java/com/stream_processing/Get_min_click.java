package com.stream_processing;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.concurrent.TimeoutException;

public class Get_min_click {
    public static void main(String[] args) throws TimeoutException, Exception {
        Filter_min_row obj = new Filter_min_row();
        obj.Write_min_click();
        obj.Filter_final_clicks();
        obj.Termination();
        obj.Close();
    }
}

class Filter_min_row {
    private SparkSession spark;
    private Kafka_df kdf_obj;
    
    public Filter_min_row() { 
        this.spark = SparkSession.builder()
                                .appName("Stream_aggregation")
                                .master("local[2]")
                                .config("spark.executor.memory", "4g")
                                .config("spark.streaming.concurrentJobs", "2") 
                                .getOrCreate();
        
        this.kdf_obj = new Kafka_df();
    }

    public void Close() { 
        this.spark.close();
    }

    public void Termination() throws StreamingQueryException { // This allows running multiple spark jobs
        this.spark.streams().awaitAnyTermination();
    }

    public void Write_min_click() throws TimeoutException, Exception { 
        Dataset<Row> click_df = kdf_obj.Read_from_kafka(this.spark, "clicks");

        click_df = this.kdf_obj.Explode_kafka_to_df(click_df, "clicks",
                                    new String[] {
                                        "Click_ID", "User_ID", "Click_time", "Click_at", 
                                        "Source", "IP"
                                    } 
                                );
        
        click_df = kdf_obj.Change_columns_type(click_df, DataTypes.TimestampType, 
                                new String[] {"click_time"}
                            );

        click_df = click_df.withWatermark("click_time", "10 minutes");
        
        // Get the user_id + min_click_time
        Dataset<Row> min_click_df = click_df.groupBy(
            functions.col("user_id").alias("min_user_id")
            ,functions.window(functions.col("click_time"), "10 minutes")
        )
        .agg(functions.min(functions.col("click_time")).alias("min_click_time"))
        .select("min_user_id", "min_click_time");

        StreamingQuery query = this.kdf_obj.Write_to_kafka(min_click_df, "min_clicks", "update",
                                    "./Checkpoints/Write_min_clicks");  
                                    
    }

    public void Filter_final_clicks() throws TimeoutException, StreamingQueryException {
        Dataset<Row> click_df = this.kdf_obj.Read_from_kafka(this.spark, "clicks, min_clicks");
                                    
        Dataset<Row> raw_click_df = this.kdf_obj.Explode_kafka_to_df(click_df, "clicks",
                                        new String[] {
                                            "Click_ID", "User_ID", "Click_time", "Click_at", 
                                            "Source", "IP"
                                        }
                                    );
        
        Dataset<Row> min_click_df = this.kdf_obj.Explode_kafka_to_df(click_df, "min_clicks",
                                        new String[] {
                                            "min_user_id", "min_click_time"
                                        }
                                    );
        
        raw_click_df = kdf_obj.Change_columns_type(raw_click_df, DataTypes.TimestampType, 
                                                    new String[] {"click_time"});
                                
        min_click_df = kdf_obj.Change_columns_type(min_click_df, DataTypes.TimestampType, 
                                                    new String[] {"min_click_time"});
        
        raw_click_df = raw_click_df.withWatermark("click_time", "10 minutes");
        min_click_df = min_click_df.withWatermark("min_click_time", "10 minutes");

        // Filter row with min_click_time for each user_id by joining 'min_clicks' with
        // original 'clicks'
        Dataset<Row> join_click_df = raw_click_df.alias("A")
                                            .join(min_click_df.alias("B"),
                                                functions.expr(
                                                    "unix_timestamp(A.click_time) = unix_timestamp(B.min_click_time) AND " +
                                                    "A.user_id = B.min_user_id "
                                                ),
                                                "inner"
                                            ).drop("timestamp_threshold", "min_click_time", "min_user_id");
                      
        StreamingQuery query = this.kdf_obj.Write_to_kafka(join_click_df, "final_clicks", "append",
                                                "./Checkpoints/Filter_final_clicks");
    }
}
