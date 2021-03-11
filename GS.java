import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class GS {
    private static class singletonHolder {
        private static GS instance = null;
    }
    private Set<String> words;
    private HashMap<String, Set<String>> pairs ;
    private HashMap<String, Boolean> values;

    public static GS getInstance() {
        if(singletonHolder.instance==null)
            singletonHolder.instance = new GS();
        return singletonHolder.instance;
    }

    private GS() {
        words= new HashSet<>();
        pairs= new HashMap<>();
        values= new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/word-relatedness.txt")));
            String line = reader.readLine();
            while (line != null) {
                String[] l = line.split("\\s+");
                words.add(l[0]);
                words.add(l[1]);
                pairs.putIfAbsent(l[0], new HashSet<String>());
                pairs.get(l[0]).add(l[1]);
                pairs.putIfAbsent(l[1], new HashSet<String>());
                pairs.get(l[1]).add(l[0]);
                boolean val = false;
                if (l[2].equals("True")){
                    val = true;
                }
                values.putIfAbsent(l[0]+" "+l[1], val);
                line = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected boolean ContainInWords(String w){
        return words.contains(w);
    }

    protected Set<String> getSetFromPairs(String w){
        return pairs.getOrDefault(w, null);
    }

    protected String getValue(String w1, String w2){
        Boolean val;
        if(values.containsKey(w1+" "+w2)) {
            val = values.get(w1 + " " + w2);
            if (val)
                return "yes";
            else
                return "no";
        }
        else if (values.containsKey(w2+" "+w1)) {
            val = values.get(w2 + " " + w1);
            if (val)
                return "yes";
            else
                return "no";
        }
        else
            return null;
    }
}
