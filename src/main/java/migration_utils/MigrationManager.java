package migration_utils;

import database.ConnectionManager;

import java.io.File;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;

public class MigrationManager {

    //TODO убрать из MigrationManager метод execute и перенети нужны методы сюда
    //TODO добавить изменение состояний при ролбеках
    //TODO изменить System.out на логгирование

    private static final String PATH_TO_MIGRATION_FOLDER = "src/main/resources/migrations";


    private MigrationManager() {
    }

    public static void migrate() {
        try {
            ensureMigrationTableExists();

            double currentVersion = getCurrentVersion();

            List<File> migrationFiles = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);

            for (File migrationFile : migrationFiles) {
                double migrationVersion = MigrationFileReader.getVersionFromFile(migrationFile);

                if (migrationVersion <= currentVersion) {
                    System.out.println("Миграция уже выполнена: " + migrationFile.getName());
                    continue;
                }


                String sql = MigrationFileReader.readSqlFromFile(migrationFile);
                recordMigration(migrationFile.getName(), MigrationStatus.IN_PROGRESS);
                try {

                    MigrationExecutor.execute(sql);

                    updateMigrationStatus(migrationFile.getName(), MigrationStatus.SUCCESS);
                    System.out.println("Миграция выполнена: " + migrationFile.getName());
                } catch (SQLException e) {
                    updateMigrationStatus(migrationFile.getName(), MigrationStatus.FAILED);
                    System.out.println("Ошибка при выполнении миграции: " + migrationFile.getName());
                    e.printStackTrace();
                    throw e;
                }
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при выполнении миграций: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void rollbackToTag(String tag){
        try (Connection connection = ConnectionManager.getConnection()) {
            connection.setAutoCommit(false);

            clearDatabase(connection);

            ensureMigrationTableExists();

            applyMigrationsUpToTag(tag);

            markMigrationsAsRevertedAfterTag(connection, tag);

            connection.commit();
            System.out.println("Откат до тега " + tag + " успешно выполнен.");

        } catch (SQLException e) {
            System.out.println("Ошибка при откате до тега: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void rollbackToDate(LocalDateTime date) {
        try (Connection connection = ConnectionManager.getConnection()) {
            connection.setAutoCommit(false);

            clearDatabase(connection);

            ensureMigrationTableExists();

            applyMigrationsUpToDate(date);

            connection.commit();
            System.out.println("Откат до даты " + date + " успешно выполнен.");

        } catch (SQLException e) {
            System.out.println("Ошибка при откате до тега: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void rollback(Double number_of_migrations) {
        try (Connection connection = ConnectionManager.getConnection()) {
            connection.setAutoCommit(false);

            clearDatabase(connection);

            ensureMigrationTableExists();

            applyMigrationsUpToChosenNumber(number_of_migrations);

            connection.commit();
            System.out.println("Откат до " + number_of_migrations + " миграций успешно выполнен.");

        } catch (SQLException e) {
            System.out.println("Ошибка при откате до заданного числа миграций: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void info() {
        String sql = "SELECT id, migration_name, executed_at, status, reverted FROM migration_history";

        try (Connection connection = ConnectionManager.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            System.out.println("Информация о миграциях:");
            System.out.println("--------------------------------------------------------------------");
            System.out.printf("%-5s %-30s %-25s %-10s %-10s%n", "ID", "Migration Name", "Executed At", "Status", "Reverted");
            System.out.println("--------------------------------------------------------------------");

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String migrationName = resultSet.getString("migration_name");
                Timestamp executedAt = resultSet.getTimestamp("executed_at");
                String status = resultSet.getString("status");
                boolean reverted = resultSet.getBoolean("reverted");

                System.out.printf("%-5d %-30s %-25s %-10s %-10s%n",
                        id, migrationName, executedAt.toLocalDateTime(), status, reverted ? "Yes" : "No");
            }

        } catch (SQLException e) {
            System.err.println("Ошибка при получении информации о миграциях: " + e.getMessage());
            e.printStackTrace();
        }
    }


    private static void applyMigrationsUpToChosenNumber(Double number_of_migrations) {
        List<File> migrationFiles = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);

        if (number_of_migrations == null || number_of_migrations <= 0 || number_of_migrations != Math.floor(number_of_migrations)) {
            throw new IllegalArgumentException("Некорректное число миграций: " + number_of_migrations);
        }

        int migrationsToApply = number_of_migrations.intValue();

        migrationFiles.sort(Comparator.comparing(File::getName));

        int appliedMigrations = 0;
        for (File migrationFile : migrationFiles) {
            if (appliedMigrations >= migrationsToApply) {
                break;
            }

            String sql = MigrationFileReader.readSqlFromFile(migrationFile);
            try {
                recordMigration(migrationFile.getName(), MigrationStatus.IN_PROGRESS);

                MigrationExecutor.execute(sql);

                updateMigrationStatus(migrationFile.getName(), MigrationStatus.SUCCESS);
            } catch (SQLException e) {
                updateMigrationStatus(migrationFile.getName(), MigrationStatus.FAILED);
                System.out.println("Ошибка при выполнении миграции: " + migrationFile.getName());
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            appliedMigrations++;

        }

        System.out.println("Применено миграций: " + appliedMigrations + " из " + migrationsToApply);
    }


    private static void applyMigrationsUpToDate(LocalDateTime date) {
        List<File> migrationFiles;

        migrationFiles = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);

        for (File migrationFile : migrationFiles) {
            LocalDateTime migrationDate = getDateFromFile(migrationFile);

            if (migrationDate.isAfter(date)) {
                System.out.println("Достигнута указанная дата " + migrationDate + ". Прекращаем выполнение миграций.");
                break;
            }

            String sql = MigrationFileReader.readSqlFromFile(migrationFile);
            try {
                recordMigration(migrationFile.getName(), MigrationStatus.IN_PROGRESS);

                MigrationExecutor.execute(sql);

                updateMigrationStatus(migrationFile.getName(), MigrationStatus.SUCCESS);
            } catch (SQLException e) {
                updateMigrationStatus(migrationFile.getName(), MigrationStatus.FAILED);
                System.out.println("Ошибка при выполнении миграции: " + migrationFile.getName());
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private static LocalDateTime getDateFromFile(File migrationFile) {
        String migrationName = migrationFile.getName();
        String sql = "SELECT executed_at FROM migration_history WHERE migration_name = ?";

        try (Connection connection = ConnectionManager.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setString(1, migrationName);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                Timestamp timestamp = resultSet.getTimestamp("executed_at");
                return timestamp.toLocalDateTime();
            } else {
                throw new RuntimeException("Дата для миграции " + migrationName + " не найдена в таблице migration_history.");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка при чтении данных из таблицы migration_history для файла " + migrationName, e);
        }
    }


    private static void applyMigrationsUpToTag(String tag) {
        List<File> migrationFiles = MigrationFileReader.findMigrationFiles(PATH_TO_MIGRATION_FOLDER);

        for (File migrationFile : migrationFiles) {
            double version = MigrationFileReader.getVersionFromFile(migrationFile);

            if (version > Double.parseDouble(tag)) {
                System.out.println("Достигнут указанный тег " + tag + ". Прекращаем выполнение миграций.");
                break;
            }

            String migrationName = migrationFile.getName();

            if (isMigrationAlreadyApplied(migrationName)) {
                System.out.println("Миграция " + migrationName + " уже выполнена. Пропускаем.");
                continue;
            }

            String sql = MigrationFileReader.readSqlFromFile(migrationFile);
            try {
                recordMigration(migrationName, MigrationStatus.IN_PROGRESS);

                MigrationExecutor.execute(sql);

                updateMigrationStatus(migrationName, MigrationStatus.SUCCESS);
            } catch (SQLException e) {
                updateMigrationStatus(migrationName, MigrationStatus.FAILED);
                System.out.println("Ошибка при выполнении миграции: " + migrationName);
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }



    private static void markMigrationsAsRevertedAfterTag(Connection connection, String tag) throws SQLException {
        String query = "UPDATE migration_history SET reverted = TRUE WHERE CAST(SUBSTRING(migration_name FROM 'V([0-9]+(\\.[0-9]+)?)') AS NUMERIC) > ? AND reverted = FALSE";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, tag);
            ps.executeUpdate();
            System.out.println("Миграции после тега " + tag + " помечены как откатанные.");
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
        migration_name VARCHAR(255) NOT NULL UNIQUE,
        executed_at TIMESTAMP NOT NULL,
        status VARCHAR(50) NOT NULL,
        reverted BOOLEAN DEFAULT FALSE
    )
    """;

        try (Connection connection = ConnectionManager.getConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(createTableQuery);
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка при проверке или создании таблицы migration_history", e);
        }
    }


    private static double getCurrentVersion() throws SQLException {
        String query = "SELECT MAX(CAST(SUBSTRING(migration_name FROM 'V([0-9]+(\\.[0-9]+)?)') AS NUMERIC)) FROM migration_history";

        try (Connection connection = ConnectionManager.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            if (resultSet.next()) {
                return resultSet.getDouble(1);
            } else {
                return 0.0;
            }
        }
    }

    private static void recordMigration(String migrationName, MigrationStatus status) {
        String query = "INSERT INTO migration_history (migration_name, executed_at, status) VALUES (?, CURRENT_TIMESTAMP, ?)";
        try (Connection connection = ConnectionManager.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, migrationName);
            statement.setString(2, status.name());
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка при записи миграции: " + migrationName, e);
        }
    }

    private static void updateMigrationStatus(String migrationName,  MigrationStatus status) {
        String query = "UPDATE migration_history SET status = ? WHERE migration_name = ?";
        try (Connection connection = ConnectionManager.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, status.name());
            statement.setString(2, migrationName);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка при обновлении статуса миграции: " + migrationName, e);
        }
    }

    private static boolean isMigrationAlreadyApplied(String migrationName) {
        String query = "SELECT COUNT(*) FROM migration_history WHERE migration_name = ? AND reverted = FALSE";
        try (Connection connection = ConnectionManager.getConnection();
             PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, migrationName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при проверке миграции: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

}



