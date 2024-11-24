package migrations;


import migration_utils.MigrationFileReader;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MigrationFileReaderTest {

    @Test
    void testGetCorrectVersion() {
        File file = new File("V10__file_to_test.sql");
        String version = MigrationFileReader.getVersionFromFile(file);
        assertEquals("10", version);
    }

    @Test
    void testThrowExceptionForInvalidDirectory() {
        String directoryPath = "invalidDirectory";
        assertThrows(
                IllegalArgumentException.class,
                () -> MigrationFileReader.findMigrationFiles(directoryPath)
        );
    }

    @Test
    void testFindMigrationFilesFromCorrectDirection() {
        String directoryPath = "migrations";
        List<File> files = MigrationFileReader.findMigrationFiles(directoryPath);

        assertNotNull(files);
        assertEquals(3, files.size());
        assertTrue(files.stream().allMatch(file -> file.getName().matches("V\\d+__.*\\.sql")));
    }

}

