import migration_utils.MigrationManager;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Scanner;

public class MigrationTool {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Введите команду или 'help' для получения списка доступных команд:");
            try (Scanner scanner = new Scanner(System.in)) {
                while (true) {
                    System.out.print("> ");
                    String input = scanner.nextLine().trim();
                    if (input.isEmpty()) {
                        continue;
                    }
                    if ("exit".equalsIgnoreCase(input)) {
                        System.out.println("Завершение работы.");
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

        try {
            switch (command) {
                case "migrate":
                    MigrationManager.migrate();
                    break;

                case "rollbackToDate":
                    if (args.length < 2) {
                        System.out.println("Ошибка: Не указана дата. Используйте формат: rollbackToDate YYYY-MM-DD");
                        printHelp();
                    } else {
                        try {
                            String dateString = args[1];
                            LocalDateTime date = LocalDate.parse(dateString).atStartOfDay();
                            MigrationManager.rollbackToDate(date);
                        } catch (DateTimeParseException e) {
                            System.out.println("Ошибка: Неверный формат даты. Используйте формат: YYYY-MM-DD");
                            printHelp();
                        }
                    }
                    break;

                case "rollbackToTag":
                    if (args.length < 2) {
                        System.out.println("Ошибка: Не указан тег. Используйте формат: rollbackToTag TAG");
                        printHelp();
                    } else {
                        String tag = args[1];
                        MigrationManager.rollbackToTag(tag);
                    }
                    break;

                case "rollback":
                    if (args.length < 2) {
                        System.out.println("Ошибка: Не указано количество миграций. Используйте формат: rollback N");
                        printHelp();
                    } else {
                        try {
                            double numberOfMigrations = Double.parseDouble(args[1]);
                            MigrationManager.rollback(numberOfMigrations);
                        } catch (NumberFormatException e) {
                            System.out.println("Ошибка: N должно быть целым числом.");
                            printHelp();
                        }
                    }
                    break;

                case "info":
                    MigrationManager.info();
                    break;

                case "help":
                    printHelp();
                    break;

                default:
                    System.out.println("Неизвестная команда: " + command);
                    printHelp();
                    break;
            }
        } catch (Exception e) {
            System.out.println("Ошибка при выполнении команды: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void printHelp() {
        System.out.println("Доступные команды:");
        System.out.println("  migrate               - Применить все миграции.");
        System.out.println("  rollbackToDate YYYY-MM-DD");
        System.out.println("                       - Откатить миграции до указанной даты.");
        System.out.println("  rollbackToTag TAG    - Откатить миграции до указанного тега.");
        System.out.println("  rollback N           - Откатить N последних миграций.");
        System.out.println("  info                 - Показать информацию о выполненных миграциях.");
        System.out.println("  help                 - Показать это сообщение.");
        System.out.println("  exit                 - Завершить работу.");
    }
}
