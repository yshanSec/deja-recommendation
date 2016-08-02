package common;

import org.apache.log4j.Logger;
import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
/**
 * Created by yishan on 1/8/16.
 */
public class ProductSearcher implements Serializable{
//    mysql connection
    static String mysqlURLPrefix = "jdbc:mysql://";
    private String ProductQuery = "select * from deja.product";
    private String[] fieldList = {"cuttings", "color", "price", "brand", "subcategory", "category"};
//    log
    private static Logger log = Logger.getLogger(ProductSearcher.class.getName());
//    product
    private HashMap<String, ProductHashMap> productMap = new HashMap<String, ProductHashMap>();

    public ProductSearcher(ConfFromProperties conf){
        Connection conn = null;
        Statement statement = null;
        String host = conf.getValue("host");
        String port = conf.getValue("port");
        String db =conf.getValue("db");
        String username = conf.getValue("username");
        String passwd = conf.getValue("passwd");
        // "jdbc:mysql://targethost:port/targetdb"
        String mysqlHost = ProductSearcher.mysqlURLPrefix.concat(host).concat(":").concat(port).concat("/").concat(db);
        //return results
        ResultSet resultSet = null;
        try{
            conn = DriverManager.getConnection(mysqlHost,username,passwd);
            statement = conn.createStatement();
            resultSet = statement.executeQuery(this.ProductQuery);
            while(resultSet.next()){
                this.productMap.put(resultSet.getString("id"),new ProductHashMap(resultSet));
            }
        } catch (SQLException e){
            this.log.error("MySQL Connection Error:" + e);
        }
    }

    private ProductHashMap fetchProductMap(String productId){
        ProductHashMap productHashMap = (ProductHashMap) this.productMap.get(productId);
        if(productHashMap==null){
            return new ProductHashMap();
        }
        else{
            return productHashMap;
        }
    }

    private boolean fieldIsValid(String field){
        for(int i =0 ; i < fieldList.length; i++){
            if(field == fieldList[i]){
                return true;
            }
        }
        return false;
    }

    public ArrayList<String> search(String productId, String field){
        if(fieldIsValid(field)){
            ProductHashMap productHashMap = fetchProductMap(productId);
            return (ArrayList<String>) productHashMap.get(field);
        }
        else{
            ArrayList<String> fieldArrayList = new ArrayList<String>();
            fieldArrayList.add(ErrorStatus.KEY_FOBIDDEN);
            return fieldArrayList;
        }
    }


}
