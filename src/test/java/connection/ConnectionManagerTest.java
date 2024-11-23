package connection;

import database.ConnectionManager;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ConnectionManagerTest {


    @Test
    public void testGetConnection() throws SQLException {
        try (Connection connection = ConnectionManager.getConnection()) {
            assertNotNull(connection);
            assertTrue(connection.isValid(2));
        }
    }

}
