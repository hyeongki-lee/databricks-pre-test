"""
ETL 처리 모듈
- YAML/JSON 기반 동적 ETL 처리
- 시작 날짜 기능 포함
- 컬럼 추가/삭제 처리
- 비식별화 (D1, D2, D3)
- 건수 체크 및 작업 기록
"""

import os
import sys
import json
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any

import yaml
import requests
import pandas as pd

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 경로 설정
BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config" / "connections.yaml"
PROFILES_DIR = BASE_DIR / "config" / "profiles"

# Databricks API 설정
DATABRICKS_HOST = "https://dbc-c6733d3c-c15f.cloud.databricks.com"
DATABRICKS_WAREHOUSE_ID = "074405e9ceba80af"
DATABRICKS_CATALOG = "workspace"

# Slack 설정
SLACK_CHANNEL = "C0C53BAG812"


def load_config():
    """설정 파일 로드"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def load_profile(engine: str, schema: str) -> Dict:
    """ETL 프로파일 로드"""
    profile_path = PROFILES_DIR / f"{engine}_{schema}.yaml"
    if not profile_path.exists():
        raise FileNotFoundError(f"프로파일을 찾을 수 없습니다: {profile_path}")

    with open(profile_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def send_slack_message(message: str):
    """Slack 메시지 전송"""
    try:
        slack_token = os.environ.get('SLACK_BOT_TOKEN')
        if not slack_token:
            logger.warning("Slack 토큰이 설정되지 않았습니다.")
            return

        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {slack_token}"},
            json={
                "channel": SLACK_CHANNEL,
                "text": message
            }
        )
        if response.status_code == 200:
            logger.info(f"Slack 메시지 전송 완료: {message[:50]}...")
        else:
            logger.error(f"Slack 메시지 전송 실패: {response.text}")
    except Exception as e:
        logger.error(f"Slack 메시지 전송 중 오류: {e}")


def execute_databricks_sql(sql: str) -> Dict:
    """Databricks SQL 실행"""
    try:
        token = os.environ.get('DATABRICKS_TOKEN')
        if not token:
            raise ValueError("Databricks 토큰이 설정되지 않았습니다.")

        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "warehouse_id": DATABRICKS_WAREHOUSE_ID,
                "statement": sql,
                "wait_timeout": "30s"
            }
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"SQL 실행 실패: {response.text}")
    except Exception as e:
        logger.error(f"Databricks SQL 실행 중 오류: {e}")
        raise


def get_source_columns(engine: str, schema: str, table: str) -> List[str]:
    """소스 테이블 컬럼 목록 조회"""
    config = load_config()

    if engine == "mysql":
        import mysql.connector
        conn = mysql.connector.connect(
            host=config['mysql']['host'],
            port=config['mysql']['port'],
            user=config['mysql']['user'],
            password=config['mysql']['password'],
            database=schema
        )
        cursor = conn.cursor()
        cursor.execute(f"SHOW COLUMNS FROM {table}")
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return columns

    elif engine == "mongodb":
        from pymongo import MongoClient
        client = MongoClient(
            host=config['mongodb']['host'],
            port=config['mongodb']['port'],
            username=config['mongodb']['user'],
            password=config['mongodb']['password'],
            authSource=config['mongodb']['auth_source']
        )
        db = client[schema]
        collection = db[table]
        # 첫 번째 문서에서 컬럼 목록 추출
        first_doc = collection.find_one()
        if first_doc:
            columns = [k for k in first_doc.keys() if k != '_id']
        else:
            columns = []
        client.close()
        return columns

    elif engine == "postgresql":
        import psycopg2
        conn = psycopg2.connect(
            host=config['postgresql']['host'],
            port=config['postgresql']['port'],
            user=config['postgresql']['user'],
            password=config['postgresql']['password']
        )
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table}'
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return columns

    return []


def check_table_exists(engine: str, schema: str, table: str) -> bool:
    """테이블 존재 여부 확인"""
    try:
        columns = get_source_columns(engine, schema, table)
        return len(columns) > 0
    except Exception:
        return False


def apply_deidentification(value: Any, method: str) -> str:
    """비식별화 적용"""
    if value is None:
        return None

    str_value = str(value)

    if method == "D1":
        # SHA2(col, 256)
        return hashlib.sha256(str_value.encode()).hexdigest()

    elif method == "D2":
        # SHA2(CONCAT('honey::', col), 256)
        salted = f"honey::{str_value}"
        return hashlib.sha256(salted.encode()).hexdigest()

    elif method == "D3":
        # Masking
        if len(str_value) <= 4:
            return "****"
        else:
            return str_value[:2] + "*" * (len(str_value) - 4) + str_value[-2:]

    return str_value


def get_target_columns(profile: Dict, table: str) -> List[str]:
    """대상 컬럼 목록 조회 (exclude_column 제외)"""
    table_config = profile.get('tables', {}).get(table, {})
    exclude_columns = table_config.get('exclude_columns', [])

    # 소스 컬럼 목록
    source_columns = table_config.get('columns', [])

    # exclude_column 제외
    target_columns = [col for col in source_columns if col not in exclude_columns]

    return target_columns


def process_table(engine: str, schema: str, table: str, profile: Dict) -> Dict:
    """단일 테이블 ETL 처리"""
    result = {
        "engine": engine,
        "schema": schema,
        "table": table,
        "status": "success",
        "message": "",
        "source_count": 0,
        "target_count": 0,
        "match": False,
        "processed_at": datetime.now().isoformat()
    }

    try:
        # 테이블 존재 여부 확인
        if not check_table_exists(engine, schema, table):
            message = f"테이블이 삭제되었습니다: {engine}.{schema}.{table}"
            logger.warning(message)
            send_slack_message(f"[ETL 경고] {message}")
            result["status"] = "skipped"
            result["message"] = message
            return result

        # 소스 컬럼 목록 조회
        source_columns = get_source_columns(engine, schema, table)

        # 프로파일에서 컬럼 정보 가져오기
        table_config = profile.get('tables', {}).get(table, {})
        profile_columns = table_config.get('columns', [])
        exclude_columns = table_config.get('exclude_columns', [])

        # 컬럼 변경 사항 확인
        added_columns = [col for col in source_columns if col not in profile_columns]
        deleted_columns = [col for col in profile_columns if col not in source_columns]

        if added_columns:
            message = f"컬럼이 추가되었습니다: {engine}.{schema}.{table} - {added_columns}"
            logger.info(message)
            send_slack_message(f"[ETL 알림] {message}")

        if deleted_columns:
            message = f"컬럼이 삭제되었습니다: {engine}.{schema}.{table} - {deleted_columns}"
            logger.info(message)
            send_slack_message(f"[ETL 알림] {message}")

        # 대상 컬럼 목록 (exclude_column 제외)
        target_columns = [col for col in source_columns if col not in exclude_columns]

        # 비식별화 설정
        deidentification = table_config.get('deidentification', {})

        # ETL 처리 유형
        etl_type = table_config.get('etl_type', 'append')

        # 시작 날짜 확인
        schedule = profile.get('schedule', {})
        start_date = schedule.get('start_date')
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if datetime.now() < start_date:
                message = f"ETL 시작 날짜가 아닙니다: {engine}.{schema}.{table} (시작: {start_date})"
                logger.info(message)
                result["status"] = "skipped"
                result["message"] = message
                return result

        # 소스 데이터 조회
        source_data = fetch_source_data(engine, schema, table, target_columns, start_date)

        # 비식별화 적용
        for col, method in deidentification.items():
            if col in target_columns:
                source_data[col] = source_data[col].apply(lambda x: apply_deidentification(x, method))

        # 대상 테이블에 적재
        target_table = f"{schema}.{table}"
        load_to_databricks(source_data, target_table, etl_type, table_config)

        # 건수 체크
        source_count = len(source_data)
        target_count = get_target_count(target_table)

        result["source_count"] = source_count
        result["target_count"] = target_count
        result["match"] = source_count == target_count

        if result["match"]:
            message = f"ETL 완료: {engine}.{schema}.{table} (건수: {source_count})"
            logger.info(message)
        else:
            message = f"ETL 건수 불일치: {engine}.{schema}.{table} (소스: {source_count}, 대상: {target_count})"
            logger.warning(message)
            send_slack_message(f"[ETL 경고] {message}")

        result["message"] = message

    except Exception as e:
        message = f"ETL 실패: {engine}.{schema}.{table} - {str(e)}"
        logger.error(message)
        send_slack_message(f"[ETL 오류] {message}")
        result["status"] = "failed"
        result["message"] = message

    return result


def fetch_source_data(engine: str, schema: str, table: str, columns: List[str], start_date: Optional[datetime] = None) -> pd.DataFrame:
    """소스 데이터 조회"""
    config = load_config()

    if engine == "mysql":
        import mysql.connector
        conn = mysql.connector.connect(
            host=config['mysql']['host'],
            port=config['mysql']['port'],
            user=config['mysql']['user'],
            password=config['mysql']['password'],
            database=schema
        )

        where_clause = ""
        if start_date:
            where_clause = f"WHERE created_at >= '{start_date.strftime('%Y-%m-%d')}'"

        query = f"SELECT {', '.join(columns)} FROM {table} {where_clause}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    elif engine == "mongodb":
        from pymongo import MongoClient
        client = MongoClient(
            host=config['mongodb']['host'],
            port=config['mongodb']['port'],
            username=config['mongodb']['user'],
            password=config['mongodb']['password'],
            authSource=config['mongodb']['auth_source']
        )
        db = client[schema]
        collection = db[table]

        query = {}
        if start_date:
            query = {"created_at": {"$gte": start_date}}

        cursor = collection.find(query, {col: 1 for col in columns})
        df = pd.DataFrame(list(cursor))
        client.close()
        return df

    elif engine == "postgresql":
        import psycopg2
        conn = psycopg2.connect(
            host=config['postgresql']['host'],
            port=config['postgresql']['port'],
            user=config['postgresql']['user'],
            password=config['postgresql']['password']
        )

        where_clause = ""
        if start_date:
            where_clause = f"WHERE created_at >= '{start_date.strftime('%Y-%m-%d')}'"

        query = f"SELECT {', '.join(columns)} FROM {schema}.{table} {where_clause}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    return pd.DataFrame()


def load_to_databricks(df: pd.DataFrame, target_table: str, etl_type: str, table_config: Dict):
    """Databricks에 데이터 적재"""
    # 임시 파일로 저장 후 COPY INTO 사용
    temp_file = f"/tmp/{target_table.replace('.', '_')}.parquet"
    df.to_parquet(temp_file, index=False)

    if etl_type == "truncate":
        # 전체 재적재
        execute_databricks_sql(f"TRUNCATE TABLE {target_table}")

    # COPY INTO 실행
    execute_databricks_sql(f"""
        COPY INTO {target_table}
        FROM '{temp_file}'
        FILEFORMAT = PARQUET
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)

    # 임시 파일 삭제
    os.remove(temp_file)


def get_target_count(target_table: str) -> int:
    """대상 테이블 건수 조회"""
    result = execute_databricks_sql(f"SELECT COUNT(*) as cnt FROM {target_table}")
    return result['result']['data_array'][0][0]


def save_check_log(result: Dict):
    """체크 로그 저장"""
    log_table = "workspace.meta.etl_check_log"

    sql = f"""
        INSERT INTO {log_table} (job_id, engine, schema_name, table_name, etl_type, source_count, target_count, match_yn, created_at)
        VALUES ('{result.get('job_id', '')}', '{result['engine']}', '{result['schema']}', '{result['table']}',
                '{result.get('etl_type', '')}', {result['source_count']}, {result['target_count']},
                '{'Y' if result['match'] else 'N'}', '{result['processed_at']}')
    """
    execute_databricks_sql(sql)


def save_run_log(result: Dict):
    """작업 기록 저장"""
    log_table = "workspace.meta.etl_run_log"

    sql = f"""
        INSERT INTO {log_table} (job_id, engine, schema_name, table_name, etl_type, status, message, created_at)
        VALUES ('{result.get('job_id', '')}', '{result['engine']}', '{result['schema']}', '{result['table']}',
                '{result.get('etl_type', '')}', '{result['status']}', '{result['message']}', '{result['processed_at']}')
    """
    execute_databricks_sql(sql)


def process_schema(engine: str, schema: str, max_workers: int = 1, job_id: str = ""):
    """스키마 단위 ETL 처리"""
    logger.info(f"ETL 처리 시작: {engine}.{schema} (동시처리: {max_workers})")

    # 프로파일 로드
    profile = load_profile(engine, schema)

    # 테이블 목록
    tables = list(profile.get('tables', {}).keys())

    results = []

    if max_workers <= 1:
        # 단일 처리
        for table in tables:
            result = process_table(engine, schema, table, profile)
            result['job_id'] = job_id
            result['etl_type'] = profile.get('tables', {}).get(table, {}).get('etl_type', 'append')
            results.append(result)

            # 로그 저장
            save_check_log(result)
            save_run_log(result)
    else:
        # 동시 처리
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_table, engine, schema, table, profile): table
                for table in tables
            }

            for future in as_completed(futures):
                table = futures[future]
                try:
                    result = future.result()
                    result['job_id'] = job_id
                    result['etl_type'] = profile.get('tables', {}).get(table, {}).get('etl_type', 'append')
                    results.append(result)

                    # 로그 저장
                    save_check_log(result)
                    save_run_log(result)
                except Exception as e:
                    logger.error(f"테이블 처리 중 오류: {table} - {e}")

    # 결과 요약
    success_count = sum(1 for r in results if r['status'] == 'success')
    fail_count = sum(1 for r in results if r['status'] == 'failed')
    skip_count = sum(1 for r in results if r['status'] == 'skipped')

    summary = f"ETL 처리 완료: {engine}.{schema} (성공: {success_count}, 실패: {fail_count}, 건너뜀: {skip_count})"
    logger.info(summary)
    send_slack_message(f"[ETL 완료] {summary}")

    return results


def main():
    """메인 함수"""
    import argparse

    parser = argparse.ArgumentParser(description='ETL 처리 모듈')
    parser.add_argument('--engine', required=True, help='엔진 (mysql, mongodb, postgresql)')
    parser.add_argument('--schema', required=True, help='스키마명')
    parser.add_argument('--workers', type=int, default=1, help='동시 처리 개수')
    parser.add_argument('--job-id', default='', help='작업 ID')

    args = parser.parse_args()

    if not args.job_id:
        args.job_id = f"ETL_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    process_schema(args.engine, args.schema, args.workers, args.job_id)


if __name__ == "__main__":
    main()
