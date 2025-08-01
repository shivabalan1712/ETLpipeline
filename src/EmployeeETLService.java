import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class EmployeeETLService {

    private static final ExecutorService exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public static CompletableFuture<List<String>> extract(String path) {
        return CompletableFuture.supplyAsync(() -> {
            try (var br = new BufferedReader(new FileReader(path))) {
                return br.lines().skip(1).collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException("Error reading file: " + e.getMessage(), e);
            }
        }, exec);
    }

    public static Employee parseLine(String line) {
        try{
            var tokens = line.split(",");
            return new Employee(
                    Integer.parseInt(tokens[0].trim()),   // id
                    tokens[1].trim(),                     // name
                    Integer.parseInt(tokens[2].trim()),   // age
                    tokens[3].trim(),                     // gender
                    tokens[4].trim(),                     // department
                    tokens[5].trim(),                     // jobTitle
                    Integer.parseInt(tokens[6].trim()),   // experienceYears
                    tokens[7].trim(),                     // education
                    tokens[8].trim(),                     // location
                    Integer.parseInt(tokens[9].trim())
            );
        } catch (Exception e){
            return null;
        }
    }

    public static CompletableFuture<List<Employee>> transform(List<String> lines){
        return CompletableFuture.supplyAsync(()->
                lines.parallelStream()
                        .map(EmployeeETLService::parseLine)
                        .filter(Objects::nonNull)
                        .toList());
    }

    public static void load(List<Employee> employees) {
        System.out.println("Parallel Analysis with Record ");
        System.out.println("Total employees:"+employees.size());

        var avgsal = employees.parallelStream()
                .mapToInt(Employee::salary)
                .average().orElse(0.0);
        System.out.println("Avg sal :"+avgsal);

        var deptcount = employees.parallelStream()
                .collect(Collectors.groupingBy(Employee::department, Collectors.counting()));
        System.out.println("Dept count :"+deptcount);

        var toppaid = employees.parallelStream()
                .max(Comparator.comparing(Employee::salary))
                .orElse(null);
        System.out.println("Highest paid : "+toppaid);

        var avgexp = employees.parallelStream()
                .mapToInt(Employee::experienceYears)
                .average().orElse(0.0);
        System.out.println("Avg exp of employees : "+avgexp);
    }

    public static CompletableFuture<List<Employee>> runETL(String path){
        return extract(path)
                .thenCompose(EmployeeETLService::transform)
                .thenApply(employees -> {
                    load(employees);
                    return employees;
                });
    }

    public static void shutdown(){
        exec.shutdown();
    }
}
