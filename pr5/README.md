инструкция по запуску решения через докер композ:

1\. создается файлы:

2\. сопускается докер композ

3\. создается база данных и заполняется тестовымиданными. Я подключила базу даннных через dbeaver и в нем сделала

CREATE TABLE users (

id SERIAL PRIMARY KEY,

name VARCHAR(100),

email VARCHAR(100),

created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE orders (

id SERIAL PRIMARY KEY,

user_id INT REFERENCES users(id),

product_name VARCHAR(100),

quantity INT,

order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

_\-- Добавление пользователей_

INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');

INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');

INSERT INTO users (name, email) VALUES ('Alice Johnson', 'alice@example.com');

INSERT INTO users (name, email) VALUES ('Bob Brown', 'bob@example.com');

_\-- Добавление заказов_

INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product A', 2);

INSERT INTO orders (user_id, product_name, quantity) VALUES (1, 'Product B', 1);

INSERT INTO orders (user_id, product_name, quantity) VALUES (2, 'Product C', 5);

INSERT INTO orders (user_id, product_name, quantity) VALUES (3, 'Product D', 3);

INSERT INTO orders (user_id, product_name, quantity) VALUES (4, 'Product E', 4);

4\. повторно запускается контейнер kafka-connector

5\. запускается kafka-consumer2 для чтения сообщений из топика

6\. переходиться в графану в ней создается новый Data sources типа Prometheus и добовляется Prometheus server URL = http://prometheus:9090

7\. добавляется новый дашборд и visualization

Описание компонентов

zookeeper - для работы с кафкой

kafka1- сама kafka

kafka-ui – для мониторинга за топиками kafka

kafka-connect

postgres – объявляение базы данных

grafana – для мониторнга kafka

kafka-consumer2 – консьюмер для чтения сообщений из топика

kafka-connector – для формирования connectors

prometheus – для grafana

kafka-connect-exporter – для метрик

запускаем просто докер консьюмер

для задания 1: добовляем значения в базу данных, запускаем kafka-connector и kafka-consumer2 для считывания сообщений.

Для задания 2: используются kafka-connect, kafka-connect-exporter, prometheus и grafana. В grafana происходит сборка дашборда.
