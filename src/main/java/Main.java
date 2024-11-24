
import static migration_utils.MigrationTool.run;

/**
 * Главный класс приложения, содержащий точку входа.
 * Запускает выполнение миграций или других команд, переданных в качестве аргументов.
 */
public class Main {
    /**
     * Точка входа в программу.
     * Принимает аргументы командной строки и передает их в метод MigrationTool.run(String[]) для обработки.
     */

    public static void main(String[] args) {
        run(args);
    }
}
