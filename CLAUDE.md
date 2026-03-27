# CLAUDE.md — Очередь платежей (Spring Boot 4 / R2DBC)

## Обзор проекта
Система обработки очереди платежей, демонстрирующая блокировку на уровне БД через `FOR UPDATE SKIP LOCKED`, обнаружение зависших задач по heartbeat и реактивное программирование (Project Reactor). Несколько воркеров конкурируют за задачи, не мешая друг другу.

## Технологический стек
- **Java 25**, **Spring Boot 4.0.0**, **Maven**
- **R2DBC** (реактивный драйвер PostgreSQL) для неблокирующего доступа к БД
- **Flyway** для миграций схемы (использует JDBC-драйвер)
- **Lombok** (`@Slf4j`, `@RequiredArgsConstructor`)
- **Testcontainers** (PostgreSQL 15) + **Reactor Test** для интеграционных тестов

## Команды сборки и тестирования
```bash
./mvnw clean install          # сборка + запуск тестов
./mvnw test                   # только тесты
./mvnw spring-boot:run        # запуск приложения (нужен локальный PostgreSQL на :5432, БД: paymentqueue, пользователь: olga)
```

## Структура проекта
```
src/main/java/com/example/paymentqueue/
├── PaymentQueueApplication.java   # точка входа, @EnableScheduling
├── model/
│   ├── PaymentTask.java           # record: данные платежа
│   ├── PaymentState.java          # enum: WAIT, PROCESSING, DONE, ERROR
│   └── PaymentTaskId.java         # record: id + retryCount (идентификатор воркера)
├── dao/
│   └── PaymentTaskDao.java        # R2DBC-запросы (skip-lock, heartbeat, сброс зависших)
├── service/
│   └── PaymentQueueService.java   # бизнес-логика, 3-шаговый захват блокировки
└── scheduler/
    └── PaymentScheduler.java      # @Scheduled воркеры: обработка очереди + сброс зависших

src/main/resources/
├── application.yml                # источники данных, flyway, конфигурация очереди
└── db/migration/
    └── V1__create_payment_task.sql # схема + тестовые данные

src/test/java/com/example/paymentqueue/
└── TwoWorkersTest.java            # интеграционные тесты (Testcontainers)
```

## Ключевые архитектурные паттерны
1. **FOR UPDATE SKIP LOCKED** — воркеры атомарно захватывают следующую задачу в состоянии WAIT, не блокируя друг друга.
2. **3-шаговый захват блокировки** (`lockNext`): SELECT FOR UPDATE → UPDATE состояния в PROCESSING → COMMIT снимает блокировку строки, состояние защищает задачу.
3. **Heartbeat** — `modified_at` обновляется во время обработки; планировщик сбрасывает задачи, зависшие более 5 минут.
4. **Счётчик повторов** — `retry_count` выступает идентификатором воркера; его инкремент аннулирует владение текущего воркера.
5. **Реактивные цепочки** — Mono/Flux повсюду; транзакции R2DBC через реактивный контекст (не ThreadLocal).

## Соглашения
- Комментарии в коде на русском языке, поясняющие работу с БД и проектные решения.
- Java records для неизменяемых DTO.
- Инлайн SQL-строки в DAO (без отдельных XML/аннотационных репозиториев).
- Короткие, точечные `@Transactional`-скоупы — только `lockNext()` транзакционный; шаги обработки используют отдельные транзакции.

## Конфигурация (application.yml)
- `payment-queue.stale-timeout-minutes: 5`
- `payment-queue.scheduler-interval-ms: 2000`
- `payment-queue.lock-retry-count: 3`

## Тестирование
- Для тестов нужен Docker (Testcontainers поднимает PostgreSQL 15).
- `TwoWorkersTest` проверяет корректность конкурентной работы воркеров и сброс зависших платежей.
