package Warehouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class Step5LoadData {

    private Connection connection;

    public Step5LoadData(String url, String user, String password) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.connection = DriverManager.getConnection(url, user, password);
            System.out.println("Database connection established.");
        } catch (Exception e) {
            System.out.println("Error establishing database connection.");
            e.printStackTrace();
        }
    }

    public void loadToDW(HashMap<Integer, String> transactionStream, HashMap<Integer, String> customerData, HashMap<Integer, String[]> productData) {
        String insertSQL = "INSERT INTO dw_transaction (order_id, order_date, product_id, customer_id, customer_name, gender, quantity_ordered, product_name, product_price, supplier_id, supplier_name, store_id, store_name, sale) " +
                           "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                           "ON DUPLICATE KEY UPDATE " +
                           "order_date = VALUES(order_date), customer_name = VALUES(customer_name), gender = VALUES(gender), " +
                           "product_name = VALUES(product_name), product_price = VALUES(product_price), sale = VALUES(sale), " +
                           "supplier_id = VALUES(supplier_id), supplier_name = VALUES(supplier_name), store_id = VALUES(store_id), store_name = VALUES(store_name)";

        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            for (Integer transactionId : transactionStream.keySet()) {
                String transactionData = transactionStream.get(transactionId);
                String[] attributes = transactionData.split(", ");
                int productId = Integer.parseInt(attributes[0].split(": ")[1]);
                int customerId = Integer.parseInt(attributes[1].split(": ")[1]);

                if (customerData.containsKey(customerId) && productData.containsKey(productId)) {
                    String customerDetails = customerData.get(customerId);
                    String customerName = customerDetails.split(", Gender: ")[0].replace("Name: ", "").trim();
                    String gender = customerDetails.split(", Gender: ")[1].trim();

                    String[] productDetails = productData.get(productId);
                    String productName = productDetails[0];
                    double productPrice = Double.parseDouble(productDetails[1]);
                    int supplierId = Integer.parseInt(productDetails[2]);
                    String supplierName = productDetails[3];
                    int storeId = Integer.parseInt(productDetails[4]);
                    String storeName = productDetails[5];

                    String fetchSQL = "SELECT order_date, quantity_ordered FROM transactions_fact WHERE transaction_id = " + transactionId;
                    Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(fetchSQL);

                    if (rs.next()) {
                        String orderDate = rs.getString("order_date");
                        int quantityOrdered = rs.getInt("quantity_ordered");

                        double sale = productPrice * quantityOrdered;

                        preparedStatement.setInt(1, transactionId);
                        preparedStatement.setString(2, orderDate); 
                        preparedStatement.setInt(3, productId);
                        preparedStatement.setInt(4, customerId);
                        preparedStatement.setString(5, customerName);
                        preparedStatement.setString(6, gender);
                        preparedStatement.setInt(7, quantityOrdered);
                        preparedStatement.setString(8, productName);
                        preparedStatement.setDouble(9, productPrice);
                        preparedStatement.setInt(10, supplierId);
                        preparedStatement.setString(11, supplierName);
                        preparedStatement.setInt(12, storeId);
                        preparedStatement.setString(13, storeName);
                        preparedStatement.setDouble(14, sale);

                        preparedStatement.executeUpdate();

                        System.out.println("Transaction ID " + transactionId + " loaded into DW with TOTAL_SALE: " + sale);
                    }
                } else {
                    System.out.println("Transaction ID " + transactionId + " skipped due to missing customer or product data.");
                }
            }
        } catch (Exception e) {
            System.out.println("Error loading data into dw_transaction.");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        String url = "jdbc:mysql://localhost:3306/proj?allowLoadLocalInfile=true";
        String user = "root"; 
        String password = "hehe1234"; 

        Step2LoadMDPartitions mdLoader = new Step2LoadMDPartitions();
        HashMap<Integer, String> customerData = mdLoader.loadAllCustomerData(url, user, password);
        HashMap<Integer, String[]> productData = mdLoader.loadAllProductData(url, user, password);

        Step1TransactionStream transactionLoader = new Step1TransactionStream();
        HashMap<Integer, String> transactionStream = transactionLoader.readTransactionSegment(url, user, password, 0);

        Step5LoadData loader = new Step5LoadData(url, user, password);
        loader.loadToDW(transactionStream, customerData, productData);
    }
}
