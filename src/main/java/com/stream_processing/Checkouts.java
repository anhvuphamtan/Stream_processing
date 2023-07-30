package com.stream_processing;

import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.*;

import java.util.concurrent.TimeoutException;

public class Checkouts {
    public static void main(String[] args) {
        Checkout_processing checkout_processing = new Checkout_processing();
        try {
            checkout_processing.main();
        } catch (StreamingQueryException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}

class Checkout_processing {
    private SparkSession spark;
    private String conn_url;
    private Kafka_df kdf_obj;

    public Checkout_processing() { 
        this.spark = SparkSession.builder()
                    .appName("Stream_processing")
                    .master("local[2]")
                    .config("spark.executor.memory", "4g") 
                    .getOrCreate();
          
        this.conn_url = "jdbc:postgresql://postgres:5432/postgres?user=postgres&password=password";
        this.kdf_obj = new Kafka_df();
    }

    public SparkSession get_spark() {
        return this.spark;
    }

    public void main() throws StreamingQueryException, TimeoutException {
        Dataset<Row> stream_df = this.kdf_obj.Read_from_kafka(this.spark, "purchases, final_clicks");
    
        Dataset<Row> User_db = this.spark.read()
                                .format("jdbc")
                                .option("url", this.conn_url)
                                .option("dbtable", "e_commerce.users")
                                .load();

        Dataset<Row> Product_db = this.spark.read()
                                .format("jdbc")
                                .option("url", this.conn_url)
                                .option("dbtable", "e_commerce.products")
                                .load();

        Dataset<Row> click_df = this.kdf_obj.Explode_kafka_to_df(stream_df, "final_clicks",
                                        new String[] {
                                            "click_id", "user_id", "click_time", "click_at", 
                                            "source", "ip"
                                        }
                                    );

        Dataset<Row> purchase_df = this.kdf_obj.Explode_kafka_to_df(stream_df, "purchases",
                                    new String[] {
                                        "Purchase_ID", "User_ID", "Product_ID", 
                                        "Shipping_cost", "Discount", "Total_cost", "Payment_method",
                                        "Quantity", "Payment_status", "Description", "Purchase_time"
                                    }
                                );
        
        click_df = kdf_obj.Change_columns_type(click_df, DataTypes.TimestampType, 
                                new String[] {"click_time"}
                            );

        purchase_df = kdf_obj.Change_columns_type(purchase_df, DataTypes.TimestampType, 
                                new String[] {"purchase_time"}
                            );

        purchase_df = kdf_obj.Change_columns_type(purchase_df, DataTypes.DoubleType, 
                                new String[] {
                                    "discount", "shipping_cost", "total_cost", "quantity"
                                }
                            );
        
        purchase_df = purchase_df.withWatermark("purchase_time", "10 minutes");
        click_df = click_df.withWatermark("click_time", "10 minutes");

       
        Dataset<Row> enrich_df = purchase_df.join(User_db, "user_id", "inner");
        enrich_df = enrich_df.join(Product_db, "product_id", "inner");
      
        enrich_df = enrich_df.withColumn("total_cost", 
                                            functions.expr("price * quantity * (1 - discount) + shipping_cost"))
                             .withColumn("profit", 
                                            functions.expr("(total_cost - shipping_cost) * commission_rate")); 

        enrich_df = enrich_df.drop("price", "discount", "shipping_cost", "commission_rate");
        
        click_df = click_df.withColumnRenamed("user_id", "click_user_id");
        
        Dataset<Row> join_df = enrich_df.alias("A")
                                    .join(click_df.alias("B"),
                                            functions.expr(
                                                "A.user_id = B.click_user_id AND " +
                                                "A.purchase_time >= B.click_time AND " +
                                                "A.purchase_time <= B.click_time + INTERVAL 10 minutes"
                                            )
                                        ).drop("click_user_id");
        
        StreamingQuery query = this.kdf_obj.Write_to_kafka(join_df, "checkouts", "append", 
                                    "./Checkpoints/Checkouts");
        
        StreamingQuery query_sink = join_df.writeStream()
                                        .foreachBatch((batchDF, batchId) -> {
                                            batchDF.write()
                                                .format("jdbc")
                                                .option("url", this.conn_url)
                                                .option("dbtable", "result.checkouts")
                                                .mode("Append")
                                                .save();
                                        })
                                        .outputMode("append")
                                        .start();
        
        this.spark.streams().awaitAnyTermination();
        this.spark.close();
    }
}
