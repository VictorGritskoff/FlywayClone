package migration_utils;

import database.ConnectionManager;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Класс, отвечающий за управление миграциями базы данных.
 * Реализует методы для выполнения миграций, откатов и получения информации о текущем статусе миграций.
 * <ul>
 *     <li>Выполнение всех миграций, которые еще не были применены.</li>
 *     <li>Получение информации о последней примененной миграции.</li>
 *     <li>Откат последних миграций.</li>
 *     <li>Откат до определенной версии (тега).</li>
 *     <li>Получение информации о текущем статусе базы данных и примененных миграциях.</li>
 * </ul>
 */
@Slf4j
public class MigrationManager {

    /**
     * Путь к директории, содержащей миграционные файлы.
     */
    private static final String PATH_TO_MIGRATION_FOLDER = "migrations";

    /**
     * Выполняет миграции, которые еще не были применены.
     * Проверяет наличие каждой миграции в базе данных и применяет те, которые еще не были выполнены.
     * Миграции выполняются в порядке возрастания версии.
     */
    public static void migrate() {
        try (Connection connection = ConnectionManager.getConnection()) {
            connection.setAutoCommit(false);

            ensureMigrationTableExists();

            List<File> migrationFiles = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);
            for (File file : migrationFiles) {
                if (!isMigrationAlreadyApplied(file, connection)) {
                    executeMigration(connection, file);
                }
            }

            connection.commit();
        } catch (Exception e) {
            log.error("Ошибка во время миграции.", e);
        }
    }

    /**
     * Выводит информацию о последней примененной миграции.
     * Запрашивает из базы данных последнюю миграцию, которая была применена, и выводит её данные.
     */
    public static void getLastAppliedMigration() {
        String query = "SELECT * FROM migration_history WHERE reverted = FALSE ORDER BY applied_at DESC LIMIT 1";
        executeQueryAndLogResults(query, "Последняя примененная миграция");
    }

    /**
     * Откатывает заданное количество последних миграций.
     * Миграции откатываются в порядке убывания их версий.
     *
     * @param numberOfMigrations Количество миграций, которые нужно откатить.
     */
    public static void rollback(int numberOfMigrations) {
        try (Connection connection = ConnectionManager.getConnection()) {
            connection.setAutoCommit(false);

            List<String> versionsToRollback = getVersionsToRollback(connection, numberOfMigrations);

            if (!versionsToRollback.isEmpty()) {
                clearDatabase(connection);
                ensureMigrationTableExists();

                List<File> migrationFiles = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);
                for (File file : migrationFiles) {
                    String version = MigrationFileReader.getVersionFromFile(file);
                    if (!versionsToRollback.contains(version) && isMigrationAlreadyApplied(file, connection)) {
                        executeMigration(connection, file);
                    }
                }
                markMigrationsAsReverted(connection, versionsToRollback);
            }

            connection.commit();
            log.info("Откат последних {} миграций выполнен.", numberOfMigrations);
        } catch (Exception e) {
            log.error("Ошибка при выполнении отката последних миграций.", e);
        }
    }

    /**
     * Откатывает миграции до определенной версии (тега).
     * Все миграции, примененные после указанного тега, будут откатаны.
     *
     * @param tag Тег, до которого нужно откатить миграции.
     */
    public static void rollbackToTag(String tag) {
        try (Connection connection = ConnectionManager.getConnection()) {
            connection.setAutoCommit(false);

            clearDatabase(connection);

            ensureMigrationTableExists();

            List<File> migrationFiles = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);
            for (File file : migrationFiles) {
                if (shouldApplyMigration(tag, file)) {
                    executeMigration(connection, file);
                } else {
                    log.info("Достигнута указанная версия {}. Остановка выполнения миграций.", tag);
                    break;
                }
            }

            markMigrationsAsRevertedAfterTag(connection, tag);
            connection.commit();
            log.info("Откат до указанной версии -- {} -- выполнен.", tag);
        } catch (Exception e) {
            log.error("Ошибка при выполнении отката до версии.", e);
        }
    }

    /**
     * Выводит информацию о текущем состоянии базы данных, включая текущую версию и список примененных миграций.
     */
    public static void info() {
        try (Connection connection = ConnectionManager.getConnection()) {
            logCurrentVersion(connection);
            logAppliedMigrations(connection);
        } catch (SQLException e) {
            log.error("Ошибка при получении статуса базы данных.", e);
        }
    }

    private static void executeMigration(Connection connection, File file) throws SQLException {
        String sql = MigrationFileReader.readSqlFromFile(file);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
        }

        String newMigration = """
                INSERT INTO migration_history (version, description, status, reverted, applied_at)
                VALUES (?, ?, ?, FALSE, CURRENT_TIMESTAMP)
                ON CONFLICT (version) DO UPDATE SET
                    description = EXCLUDED.description,
                    status = EXCLUDED.status,
                    reverted = FALSE,
                    applied_at = CASE WHEN migration_history.reverted = TRUE THEN CURRENT_TIMESTAMP ELSE migration_history.applied_at END;
            """;
        try (PreparedStatement statement = connection.prepareStatement(newMigration)) {
            statement.setString(1, MigrationFileReader.getVersionFromFile(file));
            statement.setString(2, "Migration " + file.getName());
            statement.setBoolean(3, true);
            statement.executeUpdate();
        }
    }

    private static boolean isMigrationAlreadyApplied(File migrationFile, Connection connection) throws SQLException {
        String migrationVersion = MigrationFileReader.getVersionFromFile(migrationFile);
        String query = "SELECT COUNT(*) FROM migration_history WHERE version = ? AND reverted = FALSE";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, migrationVersion);
            ResultSet rs = statement.executeQuery();
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    private static void executeQueryAndLogResults(String query, String logMessage) {
        try (Connection connection = ConnectionManager.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                String version = resultSet.getString("version");
                String description = resultSet.getString("description");
                String appliedAt = resultSet.getTimestamp("applied_at").toString();
                boolean reverted = resultSet.getBoolean("reverted");
                String status = resultSet.getString("status");

                log.info("{} - Версия: {}, Описание: {}, Дата применения: {}, Откатана: {}, Статус: {}",
                        logMessage, version, description, appliedAt, reverted ? "Да" : "Нет", status);
            } else {
                log.info("Миграции еще не применялись.");
            }
        } catch (SQLException e) {
            log.error("Ошибка при выполнении запроса к базе данных: {}", e.getMessage());
        }
    }

    private static void logCurrentVersion(Connection connection) throws SQLException {
        String query = "SELECT version FROM migration_history WHERE reverted = FALSE ORDER BY applied_at DESC LIMIT 1";
        executeQueryAndLogResults(query, "Актуальная версия базы данных");
    }

    private static void logAppliedMigrations(Connection connection) throws SQLException {
        String query = "SELECT version, description, applied_at, reverted FROM migration_history WHERE reverted = FALSE ORDER BY applied_at";
        try (PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {

            log.info("Список примененных миграций:");
            log.info(String.format("| %-20s | %-50s | %-20s | %-10s |", "Версия", "Описание", "Дата применения", "Откатана"));
            log.info("|" + "-".repeat(83) + "|");

            while (rs.next()) {
                String version = rs.getString("version");
                String description = rs.getString("description");
                String appliedAt = rs.getTimestamp("applied_at").toString();
                boolean reverted = rs.getBoolean("reverted");

                log.info(String.format("| %-20s | %-50s | %-20s | %-10s |", version, description, appliedAt, reverted ? "Да" : "Нет"));
            }

            log.info("|" + "-".repeat(83) + "|");
        }
    }

    private static List<String> getVersionsToRollback(Connection connection, int numberOfMigrations) throws SQLException {
        List<String> versionsToRollback = new ArrayList<>();
        String query = "SELECT version FROM migration_history WHERE reverted = FALSE ORDER BY version DESC LIMIT ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, numberOfMigrations);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    versionsToRollback.add(rs.getString("version"));
                }
            }
        }
        return versionsToRollback;
    }

    private static boolean shouldApplyMigration(String tag, File file) {
        return MigrationFileReader.getVersionFromFile(file).compareTo(tag) <= 0;
    }

    private static void ensureMigrationTableExists() {
        String query = "CREATE TABLE IF NOT EXISTS migration_history (" +
                "id SERIAL PRIMARY KEY, version VARCHAR(255) NOT NULL UNIQUE, description VARCHAR(255), " +
                "status BOOLEAN DEFAULT FALSE, reverted BOOLEAN DEFAULT FALSE, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
        try (Connection connection = ConnectionManager.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(query);
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка при проверке или создании таблицы migration_history", e);
        }
    }

    private static void clearDatabase(Connection connection) throws SQLException {
        String query = """
        DO $$ DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') LOOP
                EXECUTE 'TRUNCATE TABLE ' || r.table_name || ' CASCADE';
            END LOOP;
        END $$;
        """;
        try (Statement statement = connection.createStatement()) {
            statement.execute(query);
        }
    }

    private static void markMigrationsAsRevertedAfterTag(Connection connection, String tag) throws SQLException {
        String query = "UPDATE migration_history SET reverted = TRUE WHERE version > ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, tag);
            statement.executeUpdate();
        }
    }

    private static void markMigrationsAsReverted(Connection connection, List<String> versions) throws SQLException {
        String query = "UPDATE migration_history SET reverted = TRUE WHERE version IN (?)";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, String.join(",", versions));
            statement.executeUpdate();
        }
    }

}



