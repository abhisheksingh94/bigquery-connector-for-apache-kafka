import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.PrimaryKey;
public class TestBqPk {
  public static void main(String[] args) {
    try {
        StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder();
        for (java.lang.reflect.Method m : builder.getClass().getMethods()) {
          if (m.getName().toLowerCase().contains("constraints")) {
            System.out.println("StandardTableDefinition.Builder method: " + m.getName());
          }
        }
        
        Class<?> tcClass = TableConstraints.class;
        System.out.println("TableConstraints class found");
        for (java.lang.reflect.Method m : tcClass.getMethods()) {
             if (m.getName().toLowerCase().contains("primary")) {
                System.out.println("TableConstraints method: " + m.getName());
             }
        }
    } catch (Throwable t) {
        System.out.println("Error or class not found: " + t.getMessage());
    }
  }
}
