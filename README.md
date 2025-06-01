<!-- omit in toc -->
## Languages
[![python](https://img.shields.io/badge/python-3.11-d6123c?color=white&labelColor=d6123c&logo=python&logoColor=white)](https://www.python.org/)

<!-- omit in toc -->
## Frameworks
[![sqlalchemy](https://img.shields.io/badge/sqlalchemy-2.0.41-d6123c?color=white&labelColor=d6123c&logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)
[![fastapi](https://img.shields.io/badge/fastapi-0.115.12-d6123c?color=white&labelColor=d6123c&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![aiokafka](https://img.shields.io/badge/aiokafka-0.12.0-d6123c?color=white&labelColor=d6123c&logo=apachekafka&logoColor=white)](https://aiokafka.readthedocs.io/)
[![rossmann-oltp](https://img.shields.io/badge/rossmann--oltp-d6123c?color=white&labelColor=d6123c)](https://github.com/SerhiiDolhopolov/rossmann_oltp)

<!-- omit in toc -->
## Services
[![docker](https://img.shields.io/badge/docker-d6123c?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![postgreSQL](https://img.shields.io/badge/postgresql-d6123c?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)

<!-- omit in toc -->
## Table of Contents
- [Introduction](#introduction)
- [Project workflow](#project-workflow)
- [Docker Containers](#docker-containers)
- [Database Schema](#database-schema)
- [Kafka consumer](#kafka-consumer)
- [Kafka producer](#kafka-producer)
- [Getting Started](#getting-started)
- [Next Section of the Project](#next-section-of-the-project)

## Introduction
🟢 **This is part 3 of 7 Docker sections in the 🔴 [Supermarket Simulation Project](https://github.com/SerhiiDolhopolov/rossmann_services).**

🔵 [**<- Preview part with a General OLTP DB.**](https://github.com/SerhiiDolhopolov/rossmann_oltp)

## Project workflow
This section contains the simulation of five Offline Markets, each one have 3 terminals. The database is created using the [SQLAlchemy](https://www.sqlalchemy.org/) ORM. Tables with products and category info serve as cache tables for the overall database. The tables are syncronised via Sync API at the start of the work shift and then they syncronise stream data via [Kafka](https://kafka.apache.org/)

At the end of each work shift, the shop sends a report about transcations to [ClickHouse](https://clickhouse.com/). Each delivery report also sends to [ClickHouse](https://clickhouse.com/).

## Docker Containers
**This Docker section includes:**
  - **Five LOCAL OLTP DB**
    - Server for Adminer:
      - `local_db_1:5432`
    - Server for external tools:
      - `localhost:2000`
    - Other:
      - `admin`
  - **Five Apps**

## Database Schema
The schema was created at [chartdb.io](https://chartdb.io/).

**Products** and **Categories** are cached tables for General OLTP DB

![Local OLTP Schema](images/local_db.png)

## Kafka consumer
Kafka consumer is implemented via an async version - aiokafka. Kafka consumes messages from topics with update product/category info. Data modification occurs through the upsert strategy. Aiokafka hasn't included reconnect mechanism, so it was realesed.

## Kafka producer
Kafka consumer is implemented via aiokafka too. Kafka producer send messages about product quantity with some interval.

## Getting Started
**To start:**
1. Complete all steps in the [first part](https://github.com/SerhiiDolhopolov/rossmann_services).
2. Run the services:
```bash
docker compose up --build
```

## Next Section of the Project

[Rossmann Users DB](https://github.com/SerhiiDolhopolov/rossmann_users_db)
