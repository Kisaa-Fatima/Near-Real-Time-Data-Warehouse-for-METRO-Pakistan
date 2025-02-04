package Warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class product {

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/proj?allowLoadLocalInfile=true";
        String user = "root"; 
        String password = "hehe1234"; 
        String filePath = "E:/products_data.csv"; 

        try {
            
            Class.forName("com.mysql.cj.jdbc.Driver");

            
            Connection connection = DriverManager.getConnection(url, user, password);

            
            String sql = "LOAD DATA LOCAL INFILE '" + filePath.replace("\\", "/") + "' " +
                         "INTO TABLE product_data " +
                         "FIELDS TERMINATED BY ',' " +
                         "ENCLOSED BY '\"' " +
                         "LINES TERMINATED BY '\\n' " +
                         "IGNORE 1 ROWS " +
                         "(product_id, product_name, @product_price, supplier_id, supplier_name, store_id, store_name) " +
                         "SET product_price = REPLACE(@product_price, '$', '');"; 

            System.out.println("Executing SQL: " + sql); 
            Statement stmt = connection.createStatement();
            stmt.execute(sql);

            System.out.println("Data loaded successfully from products_data.csv!");

           
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
