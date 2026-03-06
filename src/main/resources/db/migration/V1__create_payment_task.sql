-- psql postgres
-- CREATE DATABASE paymentqueue;
-- \q


CREATE TYPE payment_state AS ENUM (
    'WAIT',
    'PROCESSING',
    'DONE',
    'ERROR'
);

CREATE TABLE payment_task (
    id           BIGSERIAL PRIMARY KEY,
    state        payment_state NOT NULL DEFAULT 'WAIT',
    amount       NUMERIC(19, 2) NOT NULL,
    from_account VARCHAR(20)    NOT NULL,
    to_account   VARCHAR(20)    NOT NULL,
    description  TEXT,

    -- Обновляется воркером как heartbeat "я живой".
    -- Если не обновлялась дольше порога — планировщик считает задачу зависшей.
    modified_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),

    -- Идентификатор воркера. Кэшируется при захвате.
    -- Если изменился — задача передана другому воркеру, текущий должен остановиться.
    retry_count  SMALLINT NOT NULL DEFAULT 0
);

CREATE INDEX payment_task_state_idx       ON payment_task (state);
CREATE INDEX payment_task_modified_at_idx ON payment_task (modified_at);

INSERT INTO payment_task (amount, from_account, to_account, description) VALUES
    (15000.00, '40817810000000000001', '40817810000000000002', 'Перевод другу'),
    ( 5500.50, '40817810000000000003', '40817810000000000004', 'Оплата аренды'),
    (  120.00, '40817810000000000001', '40817810000000000005', 'Коммунальные услуги'),
    (88000.00, '40817810000000000006', '40817810000000000007', 'Крупный перевод'),
    (  350.75, '40817810000000000002', '40817810000000000003', 'Возврат долга');
