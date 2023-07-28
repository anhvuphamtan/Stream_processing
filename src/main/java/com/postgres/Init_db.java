package com.postgres;

import org.apache.commons.csv.*;
import java.sql.*;
import java.io.*;

public class Init_db {
    public static void main(String[] args) throws IOException {
        try { // Wait for setting up of postgres container finish
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            Database db = new Database();
            db.Create_schema();
            db.Insert_table("e_commerce.users", "./Input_data/Users.csv");
            db.Insert_table("e_commerce.products", "./Input_data/Products.csv");
            
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

class Database {
    Connection conn;
    Statement statement;

    public Database() throws SQLException {
        String conn_url = "jdbc:postgresql://postgres:5432/postgres?user=postgres&password=password";

        conn = DriverManager.getConnection(conn_url);
        statement = this.conn.createStatement();

        System.out.println("Init connection successfully");
    }

    public void Create_schema() throws SQLException, IOException {
        BufferedReader reader = new BufferedReader(new FileReader("./src/main/java/com/postgres/create_schema.sql"));
        StringBuilder sql_statements = new StringBuilder();
        String line;

        while ((line = reader.readLine()) != null) {
            sql_statements.append(line);

            if (line.trim().endsWith(";")) {
                String complete_sql = sql_statements.toString().trim();
                
                this.statement.executeUpdate(complete_sql);
           
                sql_statements.setLength(0);
            }
        }

        System.out.println("Create schema successfully");
        reader.close();
    }

    public void Insert_table(String table_name, String file_name) throws SQLException, IOException {
        Reader reader = new FileReader(file_name);
        CSVParser csv_parser = new CSVParser(reader, CSVFormat.POSTGRESQL_CSV);
        
        Boolean skip_header = false;
        for (CSVRecord record : csv_parser) {    
            String sql_insert = String.format("INSERT INTO %s ", table_name);
            if (!skip_header) {
                skip_header = true;
                continue;
            }
            
            String values = "";
            for (String val : record) {    
                values += "\'" + val + "\'" + ",";
            }
            
            values = values.substring(0, values.length() - 1);
            
            sql_insert += "VALUES (" + values + ");";
            statement.executeUpdate(sql_insert);
        }

        reader.close();
        csv_parser.close();
        System.out.println("Insert data into table " + table_name + " successfully");
    }

    public void Querying_table(String table_name) throws SQLException, IOException {
        String sql_query = String.format("SELECT * FROM %s ;", table_name);
        ResultSet res = statement.executeQuery(sql_query);
        ResultSetMetaData meta = res.getMetaData();
        int column_cnt = meta.getColumnCount();

        while (res.next()) {
            for (int i = 1; i <= column_cnt; i++) {
                String val = res.getObject(i).toString();  
                System.out.print(val + ",");               
            }

            System.out.println("\n");
        }
    }
}

