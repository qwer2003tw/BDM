import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class DataOutput {
    DataOutput(String data,int k)
    {
        ArrayList<String> shinglingUnitDoc= new ArrayList<>();
        ArrayList<String> words= new ArrayList<>(Arrays.asList(data.split(" ")));
        for (int i=0;i<=(words.size()-k);i++){
            String temp_str="";
            for (int j=i;j<i+k;j++){
                temp_str = (new StringBuilder().append(temp_str).append(words.get(j))).toString();
                if(j!=i+k-1) temp_str = (new StringBuilder().append(temp_str).append(",")).toString();
            }
            shinglingUnitDoc.add(temp_str);
        }
        HashSet<String> hs = new HashSet<>(shinglingUnitDoc);
        shinglingUnitDoc.clear();
        shinglingUnitDoc.addAll(hs);

    }
}
