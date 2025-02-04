package Warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class Step2LoadMDPartitions {

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/proj?allowLoadLocalInfile=true";
        String user = "root"; 
        String password = "hehe1234"; 

        Step2LoadMDPartitions mdLoader = new Step2LoadMDPartitions();

        HashMap<Integer, String> customerData = mdLoader.loadAllCustomerData(url, user, password);

        HashMap<Integer, String[]> productData = mdLoader.loadAllProductData(url, user, password);

        System.out.println("Customer Data:");
        customerData.forEach((key, value) -> System.out.println("Customer ID: " + key + " -> " + value));

        System.out.println("Product Data:");
        productData.forEach((key, value) -> {
            System.out.println("Product ID: " + key + " -> Name: " + value[0] + ", Price: " + value[1] +
                               ", Supplier ID: " + value[2] + ", Supplier Name: " + value[3] +
                               ", Store ID: " + value[4] + ", Store Name: " + value[5]);
        });
    }

    public HashMap<Integer, String> loadAllCustomerData(String url, String user, String password) {
        HashMap<Integer, String> customerData = new HashMap<>();

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            Connection connection = DriverManager.getConnection(url, user, password);

            String sql = "SELECT customer_id, TRIM(customer_name) AS customer_name, TRIM(gender) AS gender FROM customer_dim";

            System.out.println("Executing SQL (All Customer Data): " + sql);

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int customerId = rs.getInt("customer_id");
                String customerName = rs.getString("customer_name");
                String gender = rs.getString("gender");

                customerData.put(customerId, "Name: " + customerName + ", Gender: " + gender);
            }
            connection.close();
        } catch (Exception e) {
            System.out.println("Error while loading all customer data");
            e.printStackTrace();
        }

        return customerData;
    }

    public HashMap<Integer, String[]> loadAllProductData(String url, String user, String password) {
        HashMap<Integer, String[]> productData = new HashMap<>();

        try {
            
            Class.forName("com.mysql.cj.jdbc.Driver");

            Connection connection = DriverManager.getConnection(url, user, password);

            String sql = "SELECT product_id, TRIM(product_name) AS product_name, product_price, supplier_id, " +
                         "TRIM(supplier_name) AS supplier_name, store_id, TRIM(store_name) AS store_name FROM product_data";

            System.out.println("Executing SQL (All Product Data): " + sql);

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                int productId = rs.getInt("product_id");
                String productName = rs.getString("product_name");
                String productPrice = String.valueOf(rs.getDouble("product_price"));
                String supplierId = String.valueOf(rs.getInt("supplier_id"));
                String supplierName = rs.getString("supplier_name");
                String storeId = String.valueOf(rs.getInt("store_id"));
                String storeName = rs.getString("store_name");

                productData.put(productId, new String[] {productName, productPrice, supplierId, supplierName, storeId, storeName});
            }

            connection.close();
        } catch (Exception e) {
            System.out.println("Error while loading all product data");
            e.printStackTrace();
        }

        return productData;
    }
}
