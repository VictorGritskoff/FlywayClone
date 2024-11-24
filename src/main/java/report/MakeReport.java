package report;

import lombok.extern.slf4j.Slf4j;
import utils.PropertiesUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

@Slf4j
public class MakeReport {

    private static final String REPORTS_DIRECTORY = "reports";
    private static final String QUERY = "SELECT id, version, description, status, reverted, applied_at FROM migration_history";

    public static void exportCsv(String fileName) {
        String filePath = prepareFilePath(fileName);
        if (filePath == null) return;

        try (ResultSet resultSet = fetchMigrationHistory();
             FileWriter fileWriter = new FileWriter(filePath)) {

            fileWriter.append("id,version,description,status,reverted,applied_at\n");

            while (resultSet.next()) {
                fileWriter.append(String.valueOf(resultSet.getInt("id")))
                        .append(',')
                        .append(resultSet.getString("version"))
                        .append(',')
                        .append(resultSet.getString("description") != null ? resultSet.getString("description") : "")
                        .append(',')
                        .append(resultSet.getBoolean("status") ? "true" : "false")
                        .append(',')
                        .append(resultSet.getBoolean("reverted") ? "true" : "false")
                        .append(',')
                        .append(resultSet.getTimestamp("applied_at").toString())
                        .append('\n');
            }

            log.info("Данные успешно экспортированы в файл {}", filePath);

        } catch (SQLException | IOException e) {
            log.error("Ошибка при экспорте данных в CSV: {}", e.getMessage(), e);
        }
    }

    public static void exportJson(String fileName) {
        String filePath = prepareFilePath(fileName);
        if (filePath == null) return;

        try (ResultSet resultSet = fetchMigrationHistory();
             FileWriter fileWriter = new FileWriter(filePath)) {

            fileWriter.append("[\n");

            boolean isFirst = true;
            while (resultSet.next()) {
                if (!isFirst) {
                    fileWriter.append(",\n");
                }
                isFirst = false;

                fileWriter.append("  {")
                        .append("\"id\": ").append(String.valueOf(resultSet.getInt("id"))).append(", ")
                        .append("\"version\": \"").append(resultSet.getString("version")).append("\", ")
                        .append("\"description\": \"")
                        .append(resultSet.getString("description") != null ? resultSet.getString("description") : "")
                        .append("\", ")
                        .append("\"status\": ").append(resultSet.getBoolean("status") ? "true" : "false").append(", ")
                        .append("\"reverted\": ").append(resultSet.getBoolean("reverted") ? "true" : "false").append(", ")
                        .append("\"applied_at\": \"").append(resultSet.getTimestamp("applied_at").toString()).append("\"")
                        .append("}");
            }

            fileWriter.append("\n]\n");

            log.info("Данные успешно экспортированы в JSON-файл {}", filePath);

        } catch (SQLException | IOException e) {
            log.error("Ошибка при экспорте данных в JSON: {}", e.getMessage(), e);
        }
    }

    private static String prepareFilePath(String fileName) {
        String reportsDirPath = new File(REPORTS_DIRECTORY).getAbsolutePath();
        File reportsDir = new File(reportsDirPath);

        if (!reportsDir.exists() && !reportsDir.mkdirs()) {
            log.error("Не удалось создать директорию {}", reportsDirPath);
            return null;
        }

        return reportsDirPath + File.separator + fileName;
    }

    private static ResultSet fetchMigrationHistory() throws SQLException {
        Connection connection = DriverManager.getConnection(
                PropertiesUtils.getProperty("db.url"),
                PropertiesUtils.getProperty("db.username"),
                PropertiesUtils.getProperty("db.password"));

        Statement statement = connection.createStatement();
        return statement.executeQuery(QUERY);
    }
}
