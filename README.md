# Graphs Warehouse & Processing System
## Архитектура. 

![архитектура проекта](architecture.jpg)
### Компоненты. 

|       Компонент       |                                             Описание                                             |
|:-------------------:|:------------------------------------------------------------------------------------------------:|
|    Kafka     |                                         Передача сообщений                                           |
|        Redis        |                    In memory KV база данных, используется для проверки сообщений на уникальность перед вставкой                    |
|     Redis init      |                                         Заполнение Redis при его старте                                           |
|    Spark     |                                         Агрегация данных из разных источников                                          |
| kafka-orientdb-sink |                                         Чтение данных из Kafka и запись в OrientDB                                          |
|      OrientDB       | Основная БД |


### Важные пути:
* Простой пример формата данных для kafka-orientdb-sink на Python - data-generator/generator.py
* Пример генератора для Spark приложения - scala-generator/
* Distributed конфиги OrientDB - orientdb/
* Схема БД - orientdb-init/setup.txt

### Демо в Docker:
- Запуск: 
  - Если нет своей managed бд и это первый запуск, инициализировать тестовую БД:\
  ``docker-compose up -d odb1 odb2 odb-init`` (подождите пока не отработает odb-init, который ставит схему)
  - Настроить `kafka-orientdb-sink` и `redis-init` в `docker-compose.yml` под свои нужды
  - Запустить остальные образы:\
  ``docker-compose up -d``
- Генерация данных: 
  - Есть вариант запустить простой генератор, минуя Spark, напрямую в топик который читается kafka-orientdb-sink:\
  ``python data-generator/generator.py``
  - Или использовать генератор для Spark из ``scala-generator/src/main/scala/MainGenerator.scala``, перед этим положив соответствующие файлы с данными из vk_data
  

UI OrientDB: [OrientDB1](http://localhost:2481)  [OrientDB2](http://localhost:2482)