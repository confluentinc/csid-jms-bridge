package io.psyncopate.util;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ExcelMerger {

    public static void main(String[] args) throws IOException {
        // Get all Excel files in the current project that start with "XYZ"
        List<File> excelFiles = getExcelFiles("logs");
        if (excelFiles.isEmpty()) {
            System.out.println("No Excel files found in folders starting with 'logs'.");
            return;
        }

        // Map to store column A values and corresponding column C values for each file
        Map<String, List<String>> dataMap = new LinkedHashMap<>();

        // Initialize workbook to write the final result
        Workbook finalWorkbook = new XSSFWorkbook();
        Sheet finalSheet = finalWorkbook.createSheet("MergedData");

        // Add header row
        Row headerRow = finalSheet.createRow(0);
        Cell headerCellA = headerRow.createCell(0);
        headerCellA.setCellValue("A");  // First column for "A"
        int colIndex = 1;

        // Process each Excel file
        for (File file : excelFiles) {
            String fileName = file.getName().replace(".xlsx", "");  // Use file name as header (without extension)

            try (FileInputStream fis = new FileInputStream(file);
                 Workbook workbook = new XSSFWorkbook(fis)) {
                Sheet sheet = workbook.getSheetAt(0);

                // Add file name as header in the final merged sheet
                Cell headerCell = headerRow.createCell(colIndex);
                headerCell.setCellValue(fileName);

                // Read rows from the current sheet
                for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                    Row row = sheet.getRow(i);
                    if (row != null) {
                        Cell cellA = row.getCell(0); // Column A
                        Cell cellC = row.getCell(2); // Column C

                        if (cellA != null && cellC != null) {
                            String key = cellA.getStringCellValue();
                            String value = cellC.getStringCellValue();

                            // Add values to the map
                            dataMap.putIfAbsent(key, new ArrayList<>(Collections.nCopies(excelFiles.size(), "")));
                            dataMap.get(key).set(colIndex - 1, value);  // Store the value in correct position
                        }
                    }
                }
            }
            colIndex++;
        }

        // Write the merged data to the final workbook
        int rowNum = 1;
        for (Map.Entry<String, List<String>> entry : dataMap.entrySet()) {
            Row row = finalSheet.createRow(rowNum++);
            row.createCell(0).setCellValue(entry.getKey());  // Column A value

            List<String> values = entry.getValue();
            for (int i = 0; i < values.size(); i++) {
                row.createCell(i + 1).setCellValue(values.get(i));  // Column C values for each file
            }
        }

        // Write the final workbook to a file
        try (FileOutputStream fos = new FileOutputStream("MergedOutput.xlsx")) {
            finalWorkbook.write(fos);
        }

        System.out.println("Merge completed.");
    }

    private static List<File> getExcelFiles(String folderPrefix) throws IOException {
        List<File> excelFiles = new ArrayList<>();

        // Walk through all directories starting from the current project folder
        Files.walk(Paths.get("/Users/ganeshprabhu/gpbase/JavaProjects/JMSBridgeTest/src/test/"))
                .filter(Files::isDirectory)  // Filter only directories
                .filter(path -> path.getFileName().toString().startsWith(folderPrefix))  // Filter directories starting with "XYZ"
                .forEach(dir -> {
                    try {

                        Files.walk(dir)  // Recursively search inside this directory
                                .filter(Files::isRegularFile)  // Only regular files
                                .filter(path -> path.getFileName().toString().endsWith(".xlsx"))  // Filter Excel files
                                .filter(path -> !path.getFileName().toString().startsWith("~$"))  // Ignore files starting with "~$"
                                .forEach(path -> {
                                    // Convert Path to File
                                    File file = path.toFile();
                                    // Check if the file is a valid Excel file
                                    try (FileInputStream fis = new FileInputStream(file)) {
                                        new XSSFWorkbook(fis);  // Try to open it to validate
                                        excelFiles.add(file);  // Add file if valid
                                    } catch (IOException e) {
                                        System.err.println("Invalid Excel file: " + file.getAbsolutePath());
                                        // Optionally, log or handle invalid file scenario
                                    }
                                });
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

        return excelFiles;
    }

}


