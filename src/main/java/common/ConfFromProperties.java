package common;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by yishan on 28/7/16.
 */
public class ConfFromProperties {
    private Properties prop;
    private InputStream inputStream;
    private static Logger log = Logger.getLogger(ConfFromProperties.class.getName());

    public ConfFromProperties(String path) throws IOException {
        // Load Configuration File
        try{
            this.prop = new Properties();
            this.inputStream = getClass().getClassLoader().getResourceAsStream(path);
            if(inputStream != null){
                this.prop.load(inputStream);
            }
        }
        catch(Exception e){
            log.error("Configuration File Error: " + e);
        }
        return;
    }
    public String getValue(String key){
        // Get value properties
        String value = "";
        try{
            value = prop.getProperty(key);
        }
        catch(Exception e){
            log.error("Key Error: "+ e);
        }
        finally {
            return value;
        }
    }
}
