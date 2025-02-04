package Warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class customer {

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/proj?allowLoadLocalInfile=true";
        String user = "root"; 
        String password = "hehe1234"; 
        String filePath = "E:/customers_data.csv"; 

        try {
            
            Class.forName("com.mysql.cj.jdbc.Driver");

            
            Connection connection = DriverManager.getConnection(url, user, password);

            // Loading data to table
            String sql = "LOAD DATA LOCAL INFILE '" + filePath.replace("\\", "/") + "' " +
                         "INTO TABLE customer_dim " +
                         "FIELDS TERMINATED BY ',' " +
                         "ENCLOSED BY '\"' " +
                         "LINES TERMINATED BY '\\n' " +
                         "IGNORE 1 ROWS " +
                         "(customer_id, customer_name, gender);"; 

            System.out.println("Executing SQL: " + sql); 
            Statement stmt = connection.createStatement();
            stmt.execute(sql);

            System.out.println("Data loaded successfully from customers_data.csv!");

            
            connection.close();
        } catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC Driver not found");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Connection failed or SQL error");
            e.printStackTrace();
        }
    }
}
