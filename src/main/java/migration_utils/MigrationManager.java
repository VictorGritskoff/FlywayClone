package migration_utils;

import database.ConnectionManager;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static migration_utils.MigrationExecutor.execute;

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
        try(Connection connection = ConnectionManager.getConnection()) {
            connection.setAutoCommit(false);

            ensureMigrationTableExists();

            List<File> migartionFilesList = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);
            for (File file : migartionFilesList) {
                if (!isMigrationAlreadyApplied(file, connection)){
                    execute(connection, file);
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

        try (Connection connection = ConnectionManager.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                ResultSet resultSet = statement.executeQuery();

                if (resultSet.next()) {
                    String version = resultSet.getString("version");
                    String description = resultSet.getString("description");
                    String appliedAt = resultSet.getTimestamp("applied_at").toString();
                    boolean reverted = resultSet.getBoolean("reverted");
                    String status = resultSet.getString("status");

                    log.info("Последняя примененная миграция - Версия: {}, Описание: {}, Дата применения: {}, Откатана: {}, Статус: {}",
                            version, description, appliedAt, reverted ? "Да" : "Нет", status);
                } else {
                    log.info("Миграции еще не применялись.");
                }
            } catch (SQLException e) {
                log.error("Ошибка при выполнении запроса к базе данных: {}", e.getMessage());
            }
        } catch (SQLException e) {
            log.error("Ошибка при подключении к базе данных: {}", e.getMessage());
        }
    }

    /**
     * Откатывает заданное количество последних миграций.
     * Миграции откатываются в порядке убывания их версий.
     *
     * @param number_of_migrations Количество миграций, которые нужно откатить.
     */
    public static void rollback(int number_of_migrations) {
        try (Connection connection = ConnectionManager.getConnection()) {
            connection.setAutoCommit(false);

            String selectQuery = """
                        SELECT id, version
                        FROM migration_history
                        WHERE reverted = FALSE
                        ORDER BY version DESC
                        LIMIT ?;
                    """;
            try (PreparedStatement statement = connection.prepareStatement(selectQuery)) {
                statement.setInt(1, number_of_migrations);
                try (ResultSet set = statement.executeQuery()) {
                    List<String> versionsToRollback = new ArrayList<>();
                    while (set.next()) {
                        versionsToRollback.add(set.getString("version"));
                    }
                    if (versionsToRollback.isEmpty()) {
                        log.info("Нет миграций для отката");
                        return;
                    }
                    clearDatabase(connection);
                    ensureMigrationTableExists();
                    List<File> migrationFiles = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);
                    for (File file : migrationFiles) {
                        String version = MigrationFileReader.getVersionFromFile(file);
                        if (!versionsToRollback.contains(version) && isMigrationAlreadyApplied(file, connection)) {
                            execute(connection, file);
                        }
                    }
                    markMigrationsAsReverted(connection, versionsToRollback);
                }
            }
            connection.commit();
            log.info("Откат последних {} миграций выполнен.", number_of_migrations);
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
    public static void rollbackToTag(String tag){
        try (Connection connection = ConnectionManager.getConnection()) {
            connection.setAutoCommit(false);

            clearDatabase(connection);

            ensureMigrationTableExists();

            List<File> migrationFiles = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);
            for (File file : migrationFiles) {
                String version = MigrationFileReader.getVersionFromFile(file);
                if (version.compareTo(tag) > 0) {
                    log.info("Достигнута указанная версия {}. Остановка выполнения миграций.", tag);
                    break;
                }
                execute(connection, file);
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
            String selectCurrentVersionQuery = """
                    SELECT version FROM migration_history
                    WHERE reverted = FALSE
                    ORDER BY version DESC LIMIT 1
                """;
            try (PreparedStatement statement = connection.prepareStatement(selectCurrentVersionQuery)) {
                ResultSet set = statement.executeQuery();
                if (set.next()) {
                    String currentVersion = set.getString("version");
                    log.info("Актуальная версия базы данных: {}", currentVersion);
                } else {
                    log.info("Миграции не применялись. База данных находится в начальном состоянии.");
                }
            }

            String allMigrationsQuery = """
                    SELECT version, description, applied_at, reverted FROM migration_history
                    WHERE reverted = FALSE
                    ORDER BY applied_at
                """;
            try (PreparedStatement ps = connection.prepareStatement(allMigrationsQuery)) {
                ResultSet rs = ps.executeQuery();

                log.info("Список примененных миграций:");
                log.info(String.format("| %-20s | %-50s | %-20s | %-10s |", "Версия", "Описание", "Дата применения", "Откатана"));
                log.info("|" + "-".repeat(83) + "|");

                while (rs.next()) {
                    String version = rs.getString("version");
                    String description = rs.getString("description");
                    String appliedAt = rs.getTimestamp("applied_at").toString();
                    boolean reverted = rs.getBoolean("reverted");

                    log.info(String.format("| %-20s | %-50s | %-20s | %-10s |",
                            version,
                            description,
                            appliedAt,
                            reverted ? "Да" : "Нет"));
                }

                log.info("|" + "-".repeat(83) + "|");
            }
        } catch (SQLException e) {
            log.error("Ошибка при получении статуса базы данных.", e);
        }
    }

    private static void markMigrationsAsRevertedAfterTag(Connection connection, String tag) throws SQLException {
        String updateQuery = "UPDATE migration_history SET reverted = TRUE WHERE version > ?";
        try (PreparedStatement ps = connection.prepareStatement(updateQuery)) {
            ps.setString(1, tag);
            ps.executeUpdate();
            log.info("Миграции после версии {} помечены как откатанные.", tag);
        }
    }

    private static void clearDatabase(Connection connection) throws SQLException {
        String query = """
        DO $$ DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'migration_history') LOOP
                EXECUTE 'TRUNCATE TABLE public.' || r.table_name || ' RESTART IDENTITY CASCADE';
            END LOOP;
        END $$;
    """;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
            System.out.println("Все данные очищены, кроме migration_history.");
        }
    }

    private static void ensureMigrationTableExists() {
        String createTableQuery = """
    CREATE TABLE IF NOT EXISTS migration_history (
        id SERIAL PRIMARY KEY,
        version VARCHAR(255) NOT NULL UNIQUE,
        description VARCHAR(255),
        status BOOLEAN DEFAULT FALSE,
        reverted BOOLEAN DEFAULT FALSE,
        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """;

        try (Connection connection = ConnectionManager.getConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(createTableQuery);
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка при проверке или создании таблицы migration_history", e);
        }
    }

    private static void markMigrationsAsReverted(Connection connection, List<String> versions) throws SQLException {
        String updateQuery = "UPDATE migration_history SET reverted = TRUE WHERE version = ?";
        try (PreparedStatement ps = connection.prepareStatement(updateQuery)) {
            for (String version : versions) {
                ps.setString(1, version);
                ps.addBatch();
            }
            int[] updateCounts = ps.executeBatch();
            log.info("Помечено как откатанные: {} миграций.", updateCounts.length);
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

}



