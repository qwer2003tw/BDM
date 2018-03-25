import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class EmployeeMinMaxCountMapper extends Mapper<Object, Text, Text, CustomMinMaxTuple> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        CustomMinMaxTuple outTuple = new CustomMinMaxTuple();
        Text departmentName = new Text();
        String data = value.toString();
        String[] field = data.split(",", -1);
        double salary = 0;
        if (null != field && field.length == 9 && field[7].length() >0) {
            salary=Double.parseDouble(field[7]);
            outTuple.setMin(salary);
            outTuple.setMax(salary);
            outTuple.setCount(1);
            departmentName.set(field[3]);
            context.write(departmentName, outTuple);
        }
    }
}