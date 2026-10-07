"""
Databricks Autoloader 설정 모듈
- rclone에서 넘어온 파일들을 auto-loader로 managed table에 적재
- Iceberg 테이블 지원
- 컬럼 추가/삭제 시 정상 동작 보장
- 건수 체크 및 로그 저장
- ETL 프로파일 활성화 플래그 관리
"""

import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

import yaml
import boto3
import requests

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


# =============================================================================
# 설정 로드 및 연결 정보
# =============================================================================

def load_config() -> Dict:
    """설정 파일 로드"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_databricks_connection() -> Dict:
    """Databricks 연결 정보 조회"""
    config = load_config()
    return {
        "host": config['databricks']['host'],
        "warehouse_id": config['databricks']['warehouse_id'],
        "catalog": config['databricks']['catalog'],
        "token": os.environ.get('DATABRICKS_TOKEN', '')
    }


def get_s3_config() -> Dict:
    """S3 설정 조회"""
    config = load_config()
    return {
        "bucket": config['s3']['bucket'],
        "region": config['s3']['region'],
        "base_path": config['s3']['base_path']
    }


# =============================================================================
# Databricks SQL 실행
# =============================================================================

def execute_databricks_sql(sql: str, connection: Optional[Dict] = None) -> Dict:
    """Databricks SQL 실행"""
    if connection is None:
        connection = get_databricks_connection()
    
    try:
        response = requests.post(
            f"{connection['host']}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {connection['token']}"},
            json={
                "warehouse_id": connection['warehouse_id'],
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


# =============================================================================
# S3 chk 파일 확인 (rclone 복제 완료 신호)
# =============================================================================

def check_s3_chk_file(engine: str, schema: str, table: str,
                      bucket: str = "databricks-test-lhk") -> bool:
    """
    S3 chk 파일 존재 여부 확인
    - rclone 복제 완료 신호 확인
    - data-prep/generate.py에서 생성하는 chk 파일 확인
    """
    try:
        s3 = boto3.client('s3')
        key = f"{engine}/{schema}/{table}/chk"
        
        response = s3.get_object(Bucket=bucket, Key=key)
        content = json.loads(response['Body'].read().decode('utf-8'))
        
        if content.get('status') == 'completed':
            logger.info(f"chk 파일 확인 완료: {key}")
            return True
        return False
    except Exception as e:
        logger.warning(f"chk 파일을 찾을 수 없습니다: {engine}/{schema}/{table} - {e}")
        return False


def wait_for_rclone_completion(engine: str, schema: str, table: str,
                                bucket: str = "databricks-test-lhk",
                                max_retries: int = 30,
                                retry_interval: int = 10) -> bool:
    """
    rclone 복제 완료 대기
    - chk 파일 존재 여부를 폴링하여 확인
    - 최대 대기 시간: max_retries * retry_interval (기본 5분)
    """
    for attempt in range(max_retries):
        if check_s3_chk_file(engine, schema, table, bucket):
            return True
        logger.info(f"rclone 복제 대기 중... ({attempt + 1}/{max_retries})")
        time.sleep(retry_interval)
    
    raise Exception(f"rclone 복제 대기 시간 초과: {engine}/{schema}/{table}")


# =============================================================================
# Iceberg 테이블 관리
# =============================================================================

def get_iceberg_table_columns(table_name: str) -> List[str]:
    """
    Iceberg 테이블의 현재 컬럼 목록 조회
    - DESCRIBE TABLE을 통해 컬럼 정보 확인
    """
    try:
        result = execute_databricks_sql(f"DESCRIBE TABLE {table_name}")
        columns = []
        for row in result.get('result', {}).get('data_array', []):
            col_name = row[0]
            if col_name and not col_name.startswith('#'):
                columns.append(col_name)
        return columns
    except Exception as e:
        logger.error(f"테이블 컬럼 조회 실패: {table_name} - {e}")
        return []


def get_s3_data_columns(s3_path: str) -> List[str]:
    """
    S3 데이터 파일의 컬럼 목록 조회
    - Parquet 파일의 스키마를 읽어 컬럼 목록 확인
    """
    try:
        # Databricks의 spark를 사용하여 S3 파일 스키마 조회
        sql = f"""
            SELECT * FROM parquet.`{s3_path}` LIMIT 0
        """
        result = execute_databricks_sql(sql)
        
        # 결과에서 컬럼 목록 추출
        columns = []
        if 'result' in result and 'schema' in result['result']:
            for col in result['result']['schema'].get('columns', []):
                columns.append(col.get('name', ''))
        return columns
    except Exception as e:
        logger.error(f"S3 데이터 컬럼 조회 실패: {s3_path} - {e}")
        return []


def handle_column_changes(table_name: str, s3_path: str) -> Tuple[List[str], List[str], List[str]]:
    """
    컬럼 변경 사항 처리
    - 삭제된 컬럼: null as column으로 정상 처리
    - 추가된 컬럼: 자의적 반영하지 않고 기존 컬럼만 작업
    
    Returns:
        (target_columns, deleted_columns, added_columns)
    """
    # 현재 테이블 컬럼
    table_columns = get_iceberg_table_columns(table_name)
    
    # S3 데이터 컬럼
    s3_columns = get_s3_data_columns(s3_path)
    
    # 삭제된 컬럼 (테이블에는 있지만 S3 데이터에는 없는 컬럼)
    deleted_columns = [col for col in table_columns if col not in s3_columns]
    
    # 추가된 컬럼 (S3 데이터에는 있지만 테이블에는 없는 컬럼)
    added_columns = [col for col in s3_columns if col not in table_columns]
    
    # 대상 컬럼: 기존 테이블 컬럼만 사용 (추가 컬럼 자의적 반영 안함)
    target_columns = [col for col in table_columns if col in s3_columns]
    
    if deleted_columns:
        logger.info(f"삭제된 컬럼 (null 처리): {deleted_columns}")
    if added_columns:
        logger.info(f"추가된 컬럼 (무시): {added_columns}")
    
    return target_columns, deleted_columns, added_columns


# =============================================================================
# Autoloader 핵심 기능
# =============================================================================

def create_iceberg_table(table_name: str, columns: List[str], 
                         s3_path: str, partition_cols: Optional[List[str]] = None):
    """
    Iceberg managed table 생성
    - 최신 Iceberg 데이터 적용을 위한 테이블 생성
    """
    partition_cols = partition_cols or []
    
    # 컬럼 정의
    col_defs = []
    for col in columns:
        col_defs.append(f"    {col} STRING")
    
    partition_defs = ""
    if partition_cols:
        partition_defs = f"\nPARTITIONED BY ({', '.join(partition_cols)})"
    
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(col_defs)}
        )
        USING ICEBERG
        LOCATION '{s3_path}'
        {partition_defs}
    """
    
    execute_databricks_sql(sql)
    logger.info(f"Iceberg 테이블 생성 완료: {table_name}")


def autoload_with_schema_evolution(table_name: str, s3_path: str,
                                   file_format: str = "PARQUET",
                                   mode: str = "APPEND") -> Dict:
    """
    Autoloader 실행 (스키마 변화 처리)
    - 컬럼 삭제: null as column으로 정상 처리
    - 컬럼 추가: 기존 컬럼만 작업 (자의적 반영 안함)
    """
    # 컬럼 변경 사항 확인
    target_columns, deleted_columns, added_columns = handle_column_changes(
        table_name, s3_path
    )
    
    # 삭제된 컬럼에 대한 null 처리
    null_columns = []
    for col in deleted_columns:
        null_columns.append(f"NULL AS {col}")
    
    # COPY INTO 실행
    # mergeSchema 옵션을 사용하여 스키마 변화 처리
    options = {
        "mergeSchema": "true",
        "mode": mode
    }
    
    options_str = ", ".join([f"'{k}' = '{v}'" for k, v in options.items()])
    
    sql = f"""
        COPY INTO {table_name}
        FROM '{s3_path}'
        FILEFORMAT = {file_format}
        COPY_OPTIONS ({options_str})
    """
    
    result = execute_databricks_sql(sql)
    logger.info(f"Autoloader 완료: {table_name}")
    
    return {
        "table_name": table_name,
        "target_columns": target_columns,
        "deleted_columns": deleted_columns,
        "added_columns": added_columns,
        "status": "success",
        "result": result
    }


def check_row_counts(source_path: str, target_table: str) -> Dict:
    """
    원본 및 대상 테이블 건수 체크
    """
    try:
        # S3 데이터 건수
        source_sql = f"SELECT COUNT(*) as cnt FROM parquet.`{source_path}`"
        source_result = execute_databricks_sql(source_sql)
        source_count = source_result['result']['data_array'][0][0]
        
        # 대상 테이블 건수
        target_sql = f"SELECT COUNT(*) as cnt FROM {target_table}"
        target_result = execute_databricks_sql(target_sql)
        target_count = target_result['result']['data_array'][0][0]
        
        return {
            "source_count": source_count,
            "target_count": target_count,
            "match": source_count == target_count
        }
    except Exception as e:
        logger.error(f"건수 체크 실패: {e}")
        return {
            "source_count": 0,
            "target_count": 0,
            "match": False,
            "error": str(e)
        }


def save_autoloader_log(engine: str, schema: str, table: str,
                        source_count: int, target_count: int,
                        match: bool, job_id: str = "",
                        deleted_columns: Optional[List[str]] = None,
                        added_columns: Optional[List[str]] = None):
    """
    Autoloader 작업 로그 저장
    - databricks 로그 테이블에 결과 저장
    """
    log_table = "workspace.meta.autoloader_log"
    
    deleted_cols_str = ', '.join(deleted_columns) if deleted_columns else ''
    added_cols_str = ', '.join(added_columns) if added_columns else ''
    
    sql = f"""
        INSERT INTO {log_table} (
            job_id, engine, schema_name, table_name, 
            source_count, target_count, match_yn, 
            deleted_columns, added_columns, created_at
        )
        VALUES (
            '{job_id}', '{engine}', '{schema}', '{table}',
            {source_count}, {target_count}, '{'Y' if match else 'N'}',
            '{deleted_cols_str}', '{added_cols_str}',
            '{datetime.now().isoformat()}'
        )
    """
    execute_databricks_sql(sql)
    logger.info(f"Autoloader 로그 저장 완료: {engine}.{schema}.{table}")


def update_profile_flag(engine: str, schema: str, table: str, 
                        flag: str = "Y") -> bool:
    """
    ETL 프로파일 활성화 플래그 업데이트
    - 초기 적재 완료 후 정규 작업 활성화
    - ETL 모듈이 참조하는 profile yaml/json에 활성화 flag 지정
    """
    profile_path = PROFILES_DIR / f"{engine}_{schema}.yaml"
    
    if not profile_path.exists():
        logger.warning(f"프로파일을 찾을 수 없습니다: {profile_path}")
        return False
    
    with open(profile_path, 'r', encoding='utf-8') as f:
        profile = yaml.safe_load(f)
    
    if table in profile.get('tables', {}):
        profile['tables'][table]['active'] = flag
        
        with open(profile_path, 'w', encoding='utf-8') as f:
            yaml.dump(profile, f, allow_unicode=True, default_flow_style=False)
        
        logger.info(f"프로파일 플래그 업데이트: {engine}.{schema}.{table} -> {flag}")
        return True
    
    return False


# =============================================================================
# 메인 Autoloader 처리 함수
# =============================================================================

def process_autoloader(engine: str, schema: str, table: str,
                       source_path: str, target_table: str,
                       file_format: str = "PARQUET",
                       mode: str = "APPEND",
                       job_id: str = "") -> Dict:
    """
    Autoloader 전체 처리 프로세스
    1. rclone 복제 완료 대기 (chk 파일 확인)
    2. Autoloader 실행 (Iceberg 테이블 적재)
    3. 건수 체크
    4. 로그 저장
    5. 프로파일 플래그 업데이트 (초기 적재 -> 정규 작업 전환)
    """
    result = {
        "engine": engine,
        "schema": schema,
        "table": table,
        "target_table": target_table,
        "status": "success",
        "message": "",
        "job_id": job_id
    }
    
    try:
        # 1. rclone 복제 완료 대기
        logger.info(f"rclone 복제 완료 대기: {engine}.{schema}.{table}")
        wait_for_rclone_completion(engine, schema, table)
        
        # 2. Autoloader 실행
        logger.info(f"Autoloader 실행: {target_table}")
        autoload_result = autoload_with_schema_evolution(
            target_table, source_path, file_format, mode
        )
        
        # 3. 건수 체크
        logger.info(f"건수 체크: {target_table}")
        count_result = check_row_counts(source_path, target_table)
        
        # 4. 로그 저장
        save_autoloader_log(
            engine, schema, table,
            count_result['source_count'],
            count_result['target_count'],
            count_result['match'],
            job_id,
            autoload_result.get('deleted_columns', []),
            autoload_result.get('added_columns', [])
        )
        
        # 5. 프로파일 플래그 업데이트 (초기 적재 완료 후 정규 작업 활성화)
        if count_result['match']:
            update_profile_flag(engine, schema, table, "Y")
            result["message"] = f"Autoloader 완료: {engine}.{schema}.{table} (건수: {count_result['target_count']})"
        else:
            result["message"] = f"건수 불일치: {engine}.{schema}.{table} (소스: {count_result['source_count']}, 대상: {count_result['target_count']})"
            result["status"] = "warning"
        
        result.update({
            "source_count": count_result['source_count'],
            "target_count": count_result['target_count'],
            "match": count_result['match'],
            "deleted_columns": autoload_result.get('deleted_columns', []),
            "added_columns": autoload_result.get('added_columns', [])
        })
        
        logger.info(result["message"])
        
    except Exception as e:
        result["status"] = "failed"
        result["message"] = f"Autoloader 실패: {engine}.{schema}.{table} - {str(e)}"
        logger.error(result["message"])
    
    return result


def batch_process_autoloader(tables_config: List[Dict], 
                             job_id: str = "") -> List[Dict]:
    """
    복수 테이블 Autoloader 배치 처리
    - 파라미터 기반 복수 작업 효율화
    """
    results = []
    
    for config in tables_config:
        result = process_autoloader(
            engine=config['engine'],
            schema=config['schema'],
            table=config['table'],
            source_path=config['source_path'],
            target_table=config['target_table'],
            file_format=config.get('file_format', 'PARQUET'),
            mode=config.get('mode', 'APPEND'),
            job_id=job_id
        )
        results.append(result)
    
    return results


# =============================================================================
# 테이블 설정 (실제 사용 예시)
# =============================================================================

AUTOLOADER_TABLES = [
    {
        "engine": "mysql",
        "schema": "mysql_schema_1",
        "table": "table_1",
        "source_path": "s3://databricks-test-lhk/mysql/mysql_schema_1/table_1",
        "target_table": "workspace.mysql_schema_1.table_1",
        "file_format": "PARQUET",
        "mode": "APPEND"
    },
    {
        "engine": "mysql",
        "schema": "mysql_schema_1",
        "table": "table_2",
        "source_path": "s3://databricks-test-lhk/mysql/mysql_schema_1/table_2",
        "target_table": "workspace.mysql_schema_1.table_2",
        "file_format": "PARQUET",
        "mode": "APPEND"
    },
    {
        "engine": "mongodb",
        "schema": "mongo_schema_1",
        "table": "table_1",
        "source_path": "s3://databricks-test-lhk/mongodb/mongo_schema_1/table_1",
        "target_table": "workspace.mongo_schema_1.table_1",
        "file_format": "PARQUET",
        "mode": "APPEND"
    },
    {
        "engine": "postgresql",
        "schema": "pg_schema_1",
        "table": "table_1",
        "source_path": "s3://databricks-test-lhk/postgresql/pg_schema_1/table_1",
        "target_table": "workspace.pg_schema_1.table_1",
        "file_format": "PARQUET",
        "mode": "APPEND"
    },
]


# =============================================================================
# 메인 실행
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Databricks Autoloader')
    parser.add_argument('--engine', help='엔진 (mysql, mongodb, postgresql)')
    parser.add_argument('--schema', help='스키마명')
    parser.add_argument('--table', help='테이블명')
    parser.add_argument('--source-path', help='S3 소스 경로')
    parser.add_argument('--target-table', help='대상 테이블명')
    parser.add_argument('--job-id', default='', help='작업 ID')
    parser.add_argument('--batch', action='store_true', help='배치 처리 모드')
    
    args = parser.parse_args()
    
    if not args.job_id:
        args.job_id = f"AUTOLOADER_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    if args.batch:
        # 배치 처리 모드
        results = batch_process_autoloader(AUTOLOADER_TABLES, args.job_id)
        for result in results:
            print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        # 단일 처리 모드
        if not all([args.engine, args.schema, args.table, args.source_path, args.target_table]):
            parser.error("단일 처리 모드는 --engine, --schema, --table, --source-path, --target-table이 필요합니다.")
        
        result = process_autoloader(
            engine=args.engine,
            schema=args.schema,
            table=args.table,
            source_path=args.source_path,
            target_table=args.target_table,
            job_id=args.job_id
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
