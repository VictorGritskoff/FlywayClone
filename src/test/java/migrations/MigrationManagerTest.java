package migrations;

import migration_utils.MigrationManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.PropertiesUtils;

import java.sql.*;

class MigrationManagerTest {

    private Connection connection;

    private static final String CREATE_TABLE_IF_NOT_EXISTS = """
        CREATE TABLE IF NOT EXISTS migration_history (
            id SERIAL PRIMARY KEY,
            version VARCHAR(255) NOT NULL UNIQUE,
            description VARCHAR(255),
            status BOOLEAN DEFAULT FALSE,
            reverted BOOLEAN DEFAULT FALSE,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """;

    private static final String CLEAR_TABLE = "TRUNCATE TABLE migration_history";

    @BeforeEach
    void setUp() throws Exception {
        System.setProperty("config.file", "application-test.properties");
        String url = PropertiesUtils.getProperty("db.url");
        String user = PropertiesUtils.getProperty("db.username");
        String password = PropertiesUtils.getProperty("db.password");

        connection = DriverManager.getConnection(url, user, password);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(CREATE_TABLE_IF_NOT_EXISTS);
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(CLEAR_TABLE);
        }
        connection.close();
    }

    @Test
    void testGetLastAppliedMigration() throws SQLException {
        String insertMigration = "INSERT INTO migration_history (version, description, status, reverted) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(insertMigration)) {
            ps.setString(1, "1.0.0");
            ps.setString(2, "Initial migration");
            ps.setBoolean(3, true);
            ps.setBoolean(4, false);
            ps.executeUpdate();
        }

        MigrationManager.getLastAppliedMigration();

        String query = "SELECT version FROM migration_history WHERE status = TRUE ORDER BY applied_at DESC LIMIT 1";
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                assert "1.0.0".equals(rs.getString("version")) : "Last applied migration version should be 1.0.0";
            } else {
                assert false : "No applied migrations found";
            }
        }
    }

    @Test
    void testRollback() throws SQLException {
        String insertMigration = "INSERT INTO migration_history (version, description, status, reverted) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(insertMigration)) {
            ps.setString(1, "1.0.0");
            ps.setString(2, "Initial migration");
            ps.setBoolean(3, true);
            ps.setBoolean(4, false);
            ps.executeUpdate();

            ps.setString(1, "1.1.0");
            ps.setString(2, "Added users table");
            ps.setBoolean(3, true);
            ps.setBoolean(4, false);
            ps.executeUpdate();
        }

        MigrationManager.rollback(1);

        String query = "SELECT COUNT(*) AS count FROM migration_history WHERE reverted = FALSE";
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            rs.next();
            assert rs.getInt("count") == 1 : "Only one migration should remain applied";
        }
    }

    @Test
    void testRollbackToTag() throws SQLException {
        String insertMigration = "INSERT INTO migration_history (version, description, status, reverted) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(insertMigration)) {
            ps.setString(1, "1.0.0");
            ps.setString(2, "Initial migration");
            ps.setBoolean(3, true);
            ps.setBoolean(4, false);
            ps.executeUpdate();

            ps.setString(1, "1.1.0");
            ps.setString(2, "Added users table");
            ps.setBoolean(3, true);
            ps.setBoolean(4, false);
            ps.executeUpdate();
        }

        MigrationManager.rollbackToTag("1.0.0");

        String query = "SELECT COUNT(*) AS count FROM migration_history WHERE reverted = FALSE AND version <= '1.0.0'";
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            rs.next();
            assert rs.getInt("count") == 1 : "Only migrations up to version 1.0.0 should remain applied";
        }
    }

    @Test
    void testInfo() throws SQLException {
        String insertMigration = "INSERT INTO migration_history (version, description, status, reverted) VALUES (?, ?, ?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(insertMigration)) {
            ps.setString(1, "1.0.0");
            ps.setString(2, "Initial migration");
            ps.setBoolean(3, true);
            ps.setBoolean(4, false);
            ps.executeUpdate();

            ps.setString(1, "1.1.0");
            ps.setString(2, "Added users table");
            ps.setBoolean(3, true);
            ps.setBoolean(4, false);
            ps.executeUpdate();
        }

        MigrationManager.info();

        String query = "SELECT COUNT(*) AS count FROM migration_history";
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            rs.next();
            assert rs.getInt("count") == 2 : "There should be 2 migrations in the database";
        }
    }
}
