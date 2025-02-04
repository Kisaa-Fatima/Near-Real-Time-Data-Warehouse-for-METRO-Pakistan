package Warehouse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class main {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/proj";
        String user = "root"; 
        String password = "hehe1234"; 

        try {
            
        	Class.forName("com.mysql.cj.jdbc.Driver");

           
            Connection connection = DriverManager.getConnection(url, user, password);

          
            Statement stmt = connection.createStatement();
            String sql = "SELECT * FROM customer_dim"; 
            stmt.executeQuery(sql);

            System.out.println("Connection successful!");

           
            connection.close();
        } catch (ClassNotFoundException e) {
            System.out.println("SQL Server JDBC Driver not found");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Connection failed");
            e.printStackTrace();
        }
    }
}