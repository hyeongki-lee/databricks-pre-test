"""
데이터 준비 스크립트
- MySQL, MongoDB, PostgreSQL에 각 4개 스키마, 스키마당 5개 테이블, 테이블당 5만 건의 샘플 데이터 생성
- 모든 테이블은 Iceberg 형식으로 생성
- 데이터 생성 후 S3에 chk 파일을 만들어서 작업 완료를 알림
"""

import os
import sys
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path

import yaml
import boto3
import pandas as pd
import numpy as np

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 경로 설정
BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config" / "connections.yaml"
S3_BASE_PATH = "s3://databricks-test-lhk"

# 엔진별 테이블 정의
TABLES_PER_SCHEMA = 5
ROWS_PER_TABLE = 50000
SCHEMAS_PER_ENGINE = 4


def load_config():
    """설정 파일 로드"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_s3_client():
    """S3 클라이언트 생성"""
    return boto3.client('s3')


def create_s3_chk_file(engine, schema, table, row_count, checksum):
    """S3에 chk 파일 생성"""
    s3 = get_s3_client()
    bucket = "databricks-test-lhk"
    key = f"{engine}/{schema}/{table}/chk"

    content = {
        "engine": engine,
        "schema": schema,
        "table": table,
        "row_count": row_count,
        "checksum": checksum,
        "created_at": datetime.now().isoformat(),
        "status": "completed"
    }

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(content, indent=2),
        ContentType='application/json'
    )
    logger.info(f"S3 chk 파일 생성 완료: {key}")


def generate_mysql_data(config):
    """MySQL 데이터 생성"""
    import mysql.connector

    mysql_config = config['mysql']
    conn = mysql.connector.connect(
        host=mysql_config['host'],
        port=mysql_config['port'],
        user=mysql_config['user'],
        password=mysql_config['password']
    )
    cursor = conn.cursor()

    for schema_info in mysql_config['schemas']:
        schema_name = schema_info['name']
        logger.info(f"MySQL 스키마 생성: {schema_name}")

        # 스키마 생성
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
        cursor.execute(f"USE {schema_name}")

        for table_idx in range(1, TABLES_PER_SCHEMA + 1):
            table_name = f"table_{table_idx}"
            logger.info(f"MySQL 테이블 생성: {schema_name}.{table_name}")

            # 테이블 생성
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    phone VARCHAR(20),
                    address VARCHAR(200),
                    age INT,
                    salary DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    description TEXT
                )
            """)

            # 샘플 데이터 생성
            logger.info(f"MySQL 데이터 생성: {schema_name}.{table_name} ({ROWS_PER_TABLE}건)")
            batch_size = 1000
            for batch_start in range(0, ROWS_PER_TABLE, batch_size):
                batch_end = min(batch_start + batch_size, ROWS_PER_TABLE)
                values = []
                for i in range(batch_start, batch_end):
                    values.append((
                        f"user_{i}",
                        f"user_{i}@example.com",
                        f"010-{i:08d}",
                        f"서울특별시 강남구 테헤란로 {i}",
                        20 + (i % 50),
                        30000000 + (i * 1000),
                        datetime.now(),
                        datetime.now(),
                        i % 2 == 0,
                        f"사용자 {i} 설명"
                    ))
                cursor.executemany(f"""
                    INSERT INTO {table_name} (name, email, phone, address, age, salary, created_at, updated_at, is_active, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, values)
                conn.commit()

            # 데이터 체크섬 계산
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
            all_data = cursor.fetchall()
            checksum = hashlib.sha256(str(all_data).encode()).hexdigest()

            # S3 chk 파일 생성
            create_s3_chk_file("mysql", schema_name, table_name, row_count, checksum)

    cursor.close()
    conn.close()
    logger.info("MySQL 데이터 생성 완료")


def generate_mongodb_data(config):
    """MongoDB 데이터 생성"""
    from pymongo import MongoClient

    mongo_config = config['mongodb']
    client = MongoClient(
        host=mongo_config['host'],
        port=mongo_config['port'],
        username=mongo_config['user'],
        password=mongo_config['password'],
        authSource=mongo_config['auth_source']
    )

    for schema_info in mongo_config['schemas']:
        schema_name = schema_info['name']
        logger.info(f"MongoDB 스키마 생성: {schema_name}")

        db = client[schema_name]

        for table_idx in range(1, TABLES_PER_SCHEMA + 1):
            table_name = f"table_{table_idx}"
            logger.info(f"MongoDB 테이블 생성: {schema_name}.{table_name}")

            collection = db[table_name]

            # 기존 데이터 삭제
            collection.delete_many({})

            # 샘플 데이터 생성
            logger.info(f"MongoDB 데이터 생성: {schema_name}.{table_name} ({ROWS_PER_TABLE}건)")
            batch_size = 1000
            for batch_start in range(0, ROWS_PER_TABLE, batch_size):
                batch_end = min(batch_start + batch_size, ROWS_PER_TABLE)
                documents = []
                for i in range(batch_start, batch_end):
                    documents.append({
                        "name": f"user_{i}",
                        "email": f"user_{i}@example.com",
                        "phone": f"010-{i:08d}",
                        "address": f"서울특별시 강남구 테헤란로 {i}",
                        "age": 20 + (i % 50),
                        "salary": 30000000 + (i * 1000),
                        "created_at": datetime.now(),
                        "updated_at": datetime.now(),
                        "is_active": i % 2 == 0,
                        "description": f"사용자 {i} 설명"
                    })
                collection.insert_many(documents)

            # 데이터 체크섬 계산
            row_count = collection.count_documents({})
            all_data = list(collection.find({}, {"_id": 0}).sort("name", 1))
            checksum = hashlib.sha256(str(all_data).encode()).hexdigest()

            # S3 chk 파일 생성
            create_s3_chk_file("mongodb", schema_name, table_name, row_count, checksum)

    client.close()
    logger.info("MongoDB 데이터 생성 완료")


def generate_postgresql_data(config):
    """PostgreSQL 데이터 생성"""
    import psycopg2

    pg_config = config['postgresql']
    conn = psycopg2.connect(
        host=pg_config['host'],
        port=pg_config['port'],
        user=pg_config['user'],
        password=pg_config['password']
    )
    conn.autocommit = True
    cursor = conn.cursor()

    for schema_info in pg_config['schemas']:
        schema_name = schema_info['name']
        logger.info(f"PostgreSQL 스키마 생성: {schema_name}")

        # 스키마 생성
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        for table_idx in range(1, TABLES_PER_SCHEMA + 1):
            table_name = f"table_{table_idx}"
            logger.info(f"PostgreSQL 테이블 생성: {schema_name}.{table_name}")

            # 테이블 생성
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    phone VARCHAR(20),
                    address VARCHAR(200),
                    age INTEGER,
                    salary NUMERIC(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    description TEXT
                )
            """)

            # 샘플 데이터 생성
            logger.info(f"PostgreSQL 데이터 생성: {schema_name}.{table_name} ({ROWS_PER_TABLE}건)")
            batch_size = 1000
            for batch_start in range(0, ROWS_PER_TABLE, batch_size):
                batch_end = min(batch_start + batch_size, ROWS_PER_TABLE)
                values = []
                for i in range(batch_start, batch_end):
                    values.append((
                        f"user_{i}",
                        f"user_{i}@example.com",
                        f"010-{i:08d}",
                        f"서울특별시 강남구 테헤란로 {i}",
                        20 + (i % 50),
                        30000000 + (i * 1000),
                        datetime.now(),
                        datetime.now(),
                        i % 2 == 0,
                        f"사용자 {i} 설명"
                    ))
                args_str = b','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", v) for v in values)
                cursor.execute(f"INSERT INTO {schema_name}.{table_name} (name, email, phone, address, age, salary, created_at, updated_at, is_active, description) VALUES " + args_str.decode())

            # 데이터 체크섬 계산
            cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
            row_count = cursor.fetchone()[0]
            cursor.execute(f"SELECT * FROM {schema_name}.{table_name} ORDER BY id")
            all_data = cursor.fetchall()
            checksum = hashlib.sha256(str(all_data).encode()).hexdigest()

            # S3 chk 파일 생성
            create_s3_chk_file("postgresql", schema_name, table_name, row_count, checksum)

    cursor.close()
    conn.close()
    logger.info("PostgreSQL 데이터 생성 완료")


def main():
    """메인 함수"""
    logger.info("데이터 준비 시작")

    config = load_config()

    # MySQL 데이터 생성
    logger.info("=" * 50)
    logger.info("MySQL 데이터 생성 시작")
    logger.info("=" * 50)
    generate_mysql_data(config)

    # MongoDB 데이터 생성
    logger.info("=" * 50)
    logger.info("MongoDB 데이터 생성 시작")
    logger.info("=" * 50)
    generate_mongodb_data(config)

    # PostgreSQL 데이터 생성
    logger.info("=" * 50)
    logger.info("PostgreSQL 데이터 생성 시작")
    logger.info("=" * 50)
    generate_postgresql_data(config)

    logger.info("=" * 50)
    logger.info("데이터 준비 완료")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
