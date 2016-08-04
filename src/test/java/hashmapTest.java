import java.util.*;

/**
 * Created by yishan on 3/8/16.
 */
public class hashmapTest {
    public static void main(String[] argv) {
//        HashMap<String, HashMap<String, String>> value = new HashMap<String, HashMap<String, String>>();
//        HashMap<String, String> innnerValue = new HashMap<String, String>();
//        innnerValue.put("keyinner","value1");
//        value.put("key1",innnerValue);
//        value.get("key1").put("keyinner1","value2");
//        System.out.println(value.get("key1").get("keyinner1"));
//        HashMap<String, ArrayList<String>> testhash = new HashMap<String, ArrayList<String>>();
//        ArrayList<String> testharray = testhash.get("test");
//        boolean a = !false && !true;
//        ArrayList<String> a = new ArrayList<String>();
//        a.add("2");
//        a.add("11");
//        Collections.sort(a);
//        for(int i = 0 ; i < a.size(); i++){
//            System.out.println(a.get(i));
//        }
//    }
//        HashMap<String, Integer> a = new HashMap<String, Integer>();
//        a.put("1", 1);
//        a.put("2", 2);
//        a.put("0", 0);
//        ArrayList<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String, Integer>>(a.entrySet());
//        for (int i = 0; i < entryList.size(); i++) {
//            Map.Entry<String, Integer> entry = entryList.get(i);
//            System.out.println(entry.getKey());
//            System.out.println(entry.getValue());
//        }
//        String a= "123";
//        String[] aList = a.split("_");
//        for(int i = 0; i< aList.length; i++){
//            System.out.println(aList[0]);
//        }

//        List<Integer> x = Arrays.asList(1, 2, 3);
//        List<Integer> y = Arrays.asList(4, 5, 6);
//        List<Integer> z = Arrays.asList(4, 5);
//        List<List<Integer>> zipped = zip(x, y, z);
//        System.out.println(zipped);
        ArrayList<Integer> a = new ArrayList<Integer>();
        ArrayList<Integer> b = new ArrayList<Integer>();
        a.add(1);
        a.add(2);
        a.add(3);
        b.add(4);
        b.add(5);
        b.add(6);
        b.add(6);
        a.retainAll(b);
        System.out.println(a);
    }
    public static <T> List<List<T>> zip(List<T>... lists) {
        List<List<T>> zipped = new ArrayList<List<T>>();
        for (List<T> list : lists) {
            for (int i = 0, listSize = list.size(); i < listSize; i++) {
                List<T> list2;
                if (i >= zipped.size())
                    zipped.add(list2 = new ArrayList<T>());
                else
                    list2 = zipped.get(i);
                list2.add(list.get(i));
            }
        }
        return zipped;
    }
}
