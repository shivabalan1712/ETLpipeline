//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.*;
//import java.util.stream.Collectors;

public class ETLpipelinenew {
// // Shared thread pool for parsing and analysis
//    private static final ExecutorService executor = Executors.newFixedThreadPool(
//            Runtime.getRuntime().availableProcessors()
//    );
//
//    // Parse single line to Employee
//    public static Employee parse(String line) {
//        String[] parts = line.split(",");
//        return new Employee(
//                Integer.parseInt(parts[0]),
//                parts[1],
//                Integer.parseInt(parts[2]),
//                parts[3],
//                parts[4],
//                parts[5],
//                Integer.parseInt(parts[6]),
//                parts[7],
//                parts[8],
//                Integer.parseInt(parts[9])
//        );
//    }
//
//    // Analyze employee list asynchronously using CompletableFutures
//    public static void analyzeParallelWithFutures(List<Employee> list) {
//        System.out.println("\n--- Parallel Analysis with Futures ---");
//        System.out.println("Total Employees: " + list.size());
//
//        long start = System.currentTimeMillis();
//
//        CompletableFuture<Void> avgFuture = CompletableFuture.runAsync(() -> {
//            double avg = list.parallelStream().mapToInt(Employee::salary).average().orElse(0);
//            System.out.printf("Average Salary: %.2f%n", avg);
//        }, executor);
//
//        CompletableFuture<Void> maxFuture = CompletableFuture.runAsync(() -> {
//            Employee highestPaid = list.parallelStream()
//                    .max(Comparator.comparingInt(Employee::salary))
//                    .orElse(null);
//            if (highestPaid != null) {
//                System.out.println("Highest Paid: " + highestPaid.name() + " (" + highestPaid.salary() + ")");
//            }
//        }, executor);
//
//        CompletableFuture<Void> countFuture = CompletableFuture.runAsync(() -> {
//            Map<String, Long> deptCount = list.parallelStream()
//                    .collect(Collectors.groupingBy(Employee::department, Collectors.counting()));
//            System.out.println("Department-wise Count: " + deptCount);
//        }, executor);
//
//        CompletableFuture.allOf(avgFuture, maxFuture, countFuture).join(); // wait for all
//        long end = System.currentTimeMillis();
//
//        System.out.println("Async Time: " + (end - start) + " ms");
//    }
//
//    // Entry point
//    public static void main(String[] args) throws Exception {
//        // Sample data path
//        String path = "/home/bat/IdeaProjects/ETLpipeline/src/Employers_data.csv"; // update path as needed
//
//        long start = System.currentTimeMillis();
//        List<String> lines = Files.readAllLines(Paths.get(path));
//
//        // Async parsing
//        CompletableFuture<List<Employee>> parsed = CompletableFuture.supplyAsync(() ->
//                lines.parallelStream()
//                        .skip(1)
//                        .map(ETLpipelinenew::parse)
//                        .toList(), executor);
//
//        // When parsing is done, analyze
//        parsed.thenAccept(ETLpipelinenew::analyzeParallelWithFutures).join();
//
//        long end = System.currentTimeMillis();
//        System.out.println("Total ETL Time: " + (end - start) + " ms");
//
//        executor.shutdown(); // shut down executor
//    }

    public static void main(String[] args) {
        var filepath = "/home/bat/IdeaProjects/ETLpipeline/src/Employers_data.csv";
        var start = System.currentTimeMillis();

        EmployeeETLService.runETL(filepath)
                .whenComplete((res,ex)->{
                    var end = System.currentTimeMillis();
                    if(ex!=null){
                        System.out.println("ETL failed.."+ex.getMessage());
                    } else {
                        System.out.println("Total ETL time : "+(end-start)+"ms");
                    }
                    EmployeeETLService.shutdown();
                }).join();
    }
}
