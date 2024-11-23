package migration_utils;

import lombok.extern.slf4j.Slf4j;
import java.util.Scanner;

/**
 * Утилита для работы с миграциями базы данных.
 * <p>
 * Этот класс предоставляет интерфейс для выполнения различных команд миграции базы данных,
 * таких как применение миграций, откат миграций по дате или тегу, а также получение информации о миграциях.
 * </p>
 */

@Slf4j
public class MigrationTool {

    /**
     * Главный метод программы. Ожидает команды от пользователя либо через аргументы,
     * либо через ввод в консоль.
     * <p>
     * Поддерживаемые команды:
     * <ul>
     *     <li>migrate - Применить все миграции.</li>
     *     <li>rollback N - Откатить N последних миграций.</li>
     *     <li>lastMigration - Показать последнюю примененную миграцию.</li>
     *     <li>rollbackToTag TAG - Откатить миграции до указанного тега.</li>
     *     <li>info - Показать информацию о выполненных миграциях.</li>
     *     <li>help - Показать список доступных команд.</li>
     *     <li>exit - Завершить работу.</li>
     * </ul>
     * </p>
     *
     */

    public static void run(String[] args) {
        if (args.length == 0) {
            System.out.println("Введите команду или 'help' для получения списка доступных команд:");
            try (Scanner scanner = new Scanner(System.in)) {
                while (true) {
                    System.out.print(">> ");
                    String input = scanner.nextLine().trim();
                    if (input.isEmpty()) {
                        continue;
                    }
                    if ("exit".equalsIgnoreCase(input)) {
                        log.info("Завершение работы.");
                        break;
                    }
                    processCommand(input.split("\\s+"));
                }
            }
        } else {
            processCommand(args);
        }
    }

    private static void processCommand(String[] args) {
        String command = args[0];
        log.debug("Получена команда: {}", command);

        try {
            switch (command) {
                case "migrate":
                    log.info("Запуск миграций...");
                    MigrationManager.migrate();
                    log.info("Миграции успешно выполнены.");
                    break;

                case "rollback":
                    if (args.length < 2) {
                        log.error("Ошибка: Не указано количество миграций. Используйте формат: rollback N");
                        printHelp();
                    } else {
                        try {
                            int numberOfMigrations = Integer.parseInt(args[1]);
                            log.info("Откатить последние {} миграций", numberOfMigrations);
                            MigrationManager.rollback(numberOfMigrations);
                        } catch (NumberFormatException e) {
                            log.error("Ошибка: N должно быть целым числом.", e);
                            printHelp();
                        }
                    }
                    break;

                case "lastMigration":
                    log.info("Получение информации о последней миграции...");
                    MigrationManager.getLastAppliedMigration();
                    break;

                case "rollbackToTag":
                    if (args.length < 2) {
                        log.error("Ошибка: Не указан тег. Используйте формат: rollbackToTag TAG");
                        printHelp();
                    } else {
                        String tag = args[1];
                        log.info("Откат миграций до тега: {}", tag);
                        MigrationManager.rollbackToTag(tag);
                    }
                    break;

                case "info":
                    log.info("Получение информации о выполненных миграциях...");
                    MigrationManager.info();
                    break;

                case "help":
                    printHelp();
                    break;

                default:
                    log.error("Неизвестная команда: {}", command);
                    printHelp();
                    break;
            }
        } catch (Exception e) {
            log.error("Ошибка при выполнении команды: {}", e.getMessage(), e);
        }
    }

    private static void printHelp() {
        System.out.println("Доступные команды:");
        System.out.println("  migrate               - Применить все миграции.");
        System.out.println("  rollback N           - Откатить N последних миграций.");
        System.out.println("  lastMigration        - Показать последнюю примененную миграцию.");
        System.out.println("  rollbackToTag TAG    - Откатить миграции до указанного тега.");
        System.out.println("  info                 - Показать информацию о выполненных миграциях.");
        System.out.println("  help                 - Показать это сообщение.");
        System.out.println("  exit                 - Завершить работу.");
    }
}
