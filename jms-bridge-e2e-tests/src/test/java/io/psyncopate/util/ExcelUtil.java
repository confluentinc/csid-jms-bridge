package io.psyncopate.util;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ExcelUtil {

    private static final String WORKBOOK_FILE_PATH;

    static {
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        WORKBOOK_FILE_PATH = Util.getLogDirectoryPath() + "/TestResults_" + timestamp + ".xlsx";
    }

    // Method to create or update a sheet with the test result of an individual test case
    public static void logTestCaseResult(String sheetName, String testCaseName, String steps, boolean result, String failureReason) {
        try (FileInputStream fis = new FileInputStream(WORKBOOK_FILE_PATH);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheet(sheetName);
            if (sheet == null) {
                // Create a new sheet if it doesn't exist
                sheet = createNewSheet(workbook, sheetName);
            }

            // Add the test result for the specific test case to the sheet
            appendTestCaseResult(sheet, testCaseName, steps, result, failureReason);

            // Auto-size columns to fit content after writing data
            autoSizeColumns(sheet);

            // Write the updated workbook back to the file
            try (FileOutputStream fos = new FileOutputStream(WORKBOOK_FILE_PATH)) {
                workbook.write(fos);
            }

        } catch (IOException e) {
            // If the workbook doesn't exist, create a new one
            createNewWorkbook(sheetName, testCaseName, steps, result, failureReason);
        }
    }

    // Method to create a new workbook and add the first sheet with the test result of an individual test case
    private static void createNewWorkbook(String sheetName, String testCaseName, String steps, boolean result, String failureReason) {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = createNewSheet(workbook, sheetName);

            // Add the test result for the specific test case to the sheet
            appendTestCaseResult(sheet, testCaseName, steps, result, failureReason);

            // Write to a new Excel file
            try (FileOutputStream fos = new FileOutputStream(WORKBOOK_FILE_PATH)) {
                workbook.write(fos);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to create a new sheet and add headers
    private static Sheet createNewSheet(Workbook workbook, String sheetName) {
        Sheet sheet = workbook.createSheet(sheetName);

        // Create header row
        Row headerRow = sheet.createRow(0);
        CellStyle headerStyle = createCellStyle(sheet, true);
        createCell(headerRow, 0, "Test Case", headerStyle);
        createCell(headerRow, 1, "Steps", headerStyle);
        createCell(headerRow, 2, "Result", headerStyle);
        createCell(headerRow, 3, "Description", headerStyle);

        return sheet;
    }

    // Method to append the test result of an individual test case to the sheet
    private static void appendTestCaseResult(Sheet sheet, String testCaseName, String steps, boolean result, String failureReason) {
        int lastRowNum = sheet.getLastRowNum();

        // Add a new row with the test case name and the result of this test run
        Row newRow = sheet.createRow(lastRowNum + 1);

        CellStyle style = createCellStyle(sheet, false); // For regular cells with vertical alignment top
        createCell(newRow, 0, testCaseName, style);
        createCell(newRow, 1, steps, style);
        createCell(newRow, 2, result ? "Pass" : "Fail", style);

        // Add failure reason if the test failed
        if (!result && failureReason != null) {
            createCell(newRow, 3, failureReason, style);
        } else {
            createCell(newRow, 3, "", style); // Leave it blank if test passed or no failure reason provided
        }

        // Adjust the row height to fit the content
        newRow.setHeight((short) -1);
    }

    // Method to create a cell with the given text and style
    private static void createCell(Row row, int column, String text, CellStyle style) {
        Cell cell = row.createCell(column);
        cell.setCellValue(text);
        cell.setCellStyle(style);
    }

    // Method to create a cell style with vertical alignment to top
    private static CellStyle createCellStyle(Sheet sheet, boolean isHeader) {
        CellStyle style = sheet.getWorkbook().createCellStyle();
        style.setWrapText(true);
        style.setVerticalAlignment(VerticalAlignment.TOP);

        if (isHeader) {
            Font font = sheet.getWorkbook().createFont();
            font.setBold(true);
            style.setFont(font);
        }

        return style;
    }
    private static void autoSizeColumns(Sheet sheet) {
        int numberOfColumns = sheet.getRow(0).getPhysicalNumberOfCells(); // Get the number of columns from the header row
        for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
            sheet.autoSizeColumn(columnIndex); // Auto-size each column
        }
    }
}
