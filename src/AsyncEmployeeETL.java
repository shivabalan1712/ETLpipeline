import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class AsyncEmployeeETL {
    public static List<String> readLines(String path) throws IOException {
        return Files.readAllLines(Paths.get(path)).stream()
                .skip(1)
                .toList();
    }

    public static Employee parse(String line) {
        String[] parts = line.split(",");
        return new Employee(
                Integer.parseInt(parts[0]),
                parts[1],
                Integer.parseInt(parts[2]),
                parts[3],
                parts[4],
                parts[5],
                Integer.parseInt(parts[6]),
                parts[7],
                parts[8],
                Integer.parseInt(parts[9])
        );
    }

    public static void analyze(String label, List<Employee> list) {
        System.out.println("\n--- " + label + " ---");
        System.out.println("Total Employees: " + list.size());
        System.out.printf("Average Salary: %.2f%n",
                list.stream().mapToInt(Employee::salary).average().orElse(0));

        Employee highestPaid = list.stream()
                .max(Comparator.comparingInt(Employee::salary))
                .orElse(null);
        System.out.println("Highest Paid: " + highestPaid.name() + " (" + highestPaid.salary() + ")");

        Map<String, Long> deptCount = list.stream()
                .collect(Collectors.groupingBy(Employee::department, Collectors.counting()));
        System.out.println("Department-wise Count: " + deptCount);
    }

    public static void singleThreaded(List<String> lines) {
        long start = System.currentTimeMillis();
        List<Employee> employees = lines.stream()
                .map(AsyncEmployeeETL::parse)
                .toList();
        long end = System.currentTimeMillis();
        analyze("Single-threaded", employees);
        System.out.println("Time: " + (end - start) + " ms");
    }

    public static void asyncCompletableFuture(List<String> lines) {
        long start = System.currentTimeMillis();
        CompletableFuture<List<Employee>> future = CompletableFuture.supplyAsync(() ->
                lines.stream().map(AsyncEmployeeETL::parse).toList()
        );
        List<Employee> employees = future.join();
        long end = System.currentTimeMillis();
        analyze("CompletableFuture (Async)", employees);
        System.out.println("Time: " + (end - start) + " ms");
    }

    public static void parallelStream(List<String> lines) {
        long start = System.currentTimeMillis();
        List<Employee> employees = lines.parallelStream()
                .map(AsyncEmployeeETL::parse)
                .toList();
        long end = System.currentTimeMillis();
        analyze("Parallel Stream", employees);
        System.out.println("Time: " + (end - start) + " ms");
    }

    public static void main(String[] args) throws IOException {
        String filePath = "/home/bat/IdeaProjects/ETLpipeline/src/Employers_data.csv";
        List<String> lines = readLines(filePath);

        singleThreaded(lines);
        asyncCompletableFuture(lines);
        parallelStream(lines);
    }
}
