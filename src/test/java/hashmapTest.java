import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Created by yishan on 3/8/16.
 */
public class hashmapTest {
    public static void main(String[] argv){
//        HashMap<String, HashMap<String, String>> value = new HashMap<String, HashMap<String, String>>();
//        HashMap<String, String> innnerValue = new HashMap<String, String>();
//        innnerValue.put("keyinner","value1");
//        value.put("key1",innnerValue);
//        value.get("key1").put("keyinner1","value2");
//        System.out.println(value.get("key1").get("keyinner1"));
//        HashMap<String, ArrayList<String>> testhash = new HashMap<String, ArrayList<String>>();
//        ArrayList<String> testharray = testhash.get("test");
//        boolean a = !false && !true;
        ArrayList<String> a = new ArrayList<String>();
        a.add("2");
        a.add("11");
        Collections.sort(a);
        for(int i = 0 ; i < a.size(); i++){
            System.out.println(a.get(i));
        }
    }
}
