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

    public static void analyzeSingle(List<Employee> list) {
        System.out.println("\n--- Single-threaded Analysis ---");
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

    public static void analyzeParallel(List<Employee> list) {
        System.out.println("\n--- Parallel Analysis ---");
        System.out.println("Total Employees: " + list.size());

        System.out.printf("Average Salary: %.2f%n",
                list.parallelStream().mapToInt(Employee::salary).average().orElse(0));

        Employee highestPaid = list.parallelStream()
                .max(Comparator.comparingInt(Employee::salary))
                .orElse(null);
        System.out.println("Highest Paid: " + highestPaid.name() + " (" + highestPaid.salary() + ")");

        Map<String, Long> deptCount = list.parallelStream()
                .collect(Collectors.groupingBy(Employee::department, Collectors.counting()));
        System.out.println("Department-wise Count: " + deptCount);
    }

    // Add this at the class level (before any method)
    private static final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());


    public static void analyzeAsync(List<Employee> list) {
        System.out.println("\n--- Async Analysis (CompletableFuture + Custom Executor) ---");
        System.out.println("Total Employees: " + list.size());
        long start = System.currentTimeMillis();

        CompletableFuture<Double> avgSalaryFuture = CompletableFuture.supplyAsync(() ->
                list.parallelStream().mapToInt(Employee::salary).average().orElse(0), executor);

        CompletableFuture<Employee> highestPaidFuture = CompletableFuture.supplyAsync(() ->
                list.parallelStream().max(Comparator.comparingInt(Employee::salary)).orElse(null), executor);

        CompletableFuture<Map<String, Long>> deptCountFuture = CompletableFuture.supplyAsync(() ->
                list.parallelStream().collect(Collectors.groupingByConcurrent(Employee::department, Collectors.counting())), executor);

        CompletableFuture<Void> finalResult = avgSalaryFuture.thenAcceptBothAsync(highestPaidFuture, (avg, highest) -> {
            System.out.printf("Average Salary: %.2f%n", avg);
            System.out.println("Highest Paid: " + highest.name() + " (" + highest.salary() + ")");
        }, executor).thenCombineAsync(deptCountFuture, (unused, deptCount) -> {
            System.out.println("Department-wise Count: " + deptCount);
            return null;
        }, executor);

        finalResult.join();
        long end = System.currentTimeMillis();
        System.out.println("Async Time: " + (end - start) + " ms");
    }

    public static void analyzeParallelWithFutures(List<Employee> list) {
        System.out.println("\n--- Parallel Analysis with Futures ---");
        System.out.println("Total Employees: " + list.size());

        ExecutorService exec = Executors.newFixedThreadPool(3); // Run 3 tasks in parallel

        CompletableFuture<Void> avgFuture = CompletableFuture.runAsync(() -> {
            double avg = list.parallelStream().mapToInt(Employee::salary).average().orElse(0);
            System.out.printf("Average Salary: %.2f%n", avg);
        }, exec);

        CompletableFuture<Void> maxFuture = CompletableFuture.runAsync(() -> {
            Employee highestPaid = list.parallelStream()
                    .max(Comparator.comparingInt(Employee::salary))
                    .orElse(null);
            System.out.println("Highest Paid: " + highestPaid.name() + " (" + highestPaid.salary() + ")");
        }, exec);

        CompletableFuture<Void> countFuture = CompletableFuture.runAsync(() -> {
            Map<String, Long> deptCount = list.parallelStream()
                    .collect(Collectors.groupingBy(Employee::department, Collectors.counting()));
            System.out.println("Department-wise Count: " + deptCount);
        }, exec);

        CompletableFuture.allOf(avgFuture, maxFuture, countFuture).join(); // Wait for all
        exec.shutdown();
    }


    public static void asyncOptimized(List<String> lines) {
        long start = System.currentTimeMillis();
        CompletableFuture<List<Employee>> future = CompletableFuture.supplyAsync(() ->
                lines.parallelStream().map(AsyncEmployeeETL::parse).toList(), executor);
        List<Employee> employees = future.join();
        long end = System.currentTimeMillis();
        analyzeAsync(employees);
        System.out.println("ETL + Async Analysis Time: " + (end - start) + " ms");
    }

    public static void warmup(List<String> lines) {
        // Warmup JVM (optional, improves JIT and thread pool readiness)
        for (int i = 0; i < 3; i++) {
            lines.parallelStream().map(AsyncEmployeeETL::parse).toList();
        }
    }
    public static void singleThreaded(List<String> lines) {
        long start = System.currentTimeMillis();
        List<Employee> employees = lines.stream()
                .map(AsyncEmployeeETL::parse)
                .toList();
        long end = System.currentTimeMillis();
        analyzeSingle(employees);
        System.out.println("Time: " + (end - start) + " ms");
    }

    public static void parallelStream(List<String> lines) {
        long start = System.currentTimeMillis();
        List<Employee> employees = lines.parallelStream()
                .map(AsyncEmployeeETL::parse)
                .toList();
        long end = System.currentTimeMillis();
        analyzeParallel(employees);
        System.out.println("Time: " + (end - start) + " ms");
    }

    public static void asyncCompletableFuture(List<String> lines) {
        long start = System.currentTimeMillis();
        CompletableFuture<List<Employee>> future = CompletableFuture.supplyAsync(() ->
                lines.stream().map(AsyncEmployeeETL::parse).toList()
        );
        List<Employee> employees = future.join();
        long end = System.currentTimeMillis();
        analyzeAsync(employees);
        System.out.println("Time: " + (end - start) + " ms");
    }


    public static void main(String[] args) throws IOException {
        String filePath = "/home/bat/IdeaProjects/ETLpipeline/src/Employers_data.csv";
        List<String> lines = readLines(filePath);
        warmup(lines);

        singleThreaded(lines);
        asyncOptimized(lines);
        parallelStream(lines);
        List<Employee> employees = lines.stream().map(AsyncEmployeeETL::parse).toList();
        analyzeParallelWithFutures(employees);
        executor.shutdown(); // Graceful shutdown

    }
}
