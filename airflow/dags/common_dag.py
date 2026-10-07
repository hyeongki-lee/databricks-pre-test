"""
Airflow 공통 DAG 모듈
- 재사용 가능한 공통 DAG 템플릿 제공
- 파라미터 기반 DAG 생성을 통한 확장성 및 재사용성 확보
- 복수 작업 효율화를 위한 배치 처리 지원
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from functools import wraps

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# 로깅 설정
logger = logging.getLogger(__name__)

# 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "connections.yaml")


# =============================================================================
# 공통 함수 및 유틸리티
# =============================================================================

def load_config() -> Dict:
    """설정 파일 로드"""
    import yaml
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


def execute_databricks_sql(sql: str, connection: Optional[Dict] = None) -> Dict:
    """Databricks SQL 실행 (공통 함수)"""
    import requests
    
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


def send_slack_notification(message: str, level: str = "info"):
    """Slack 알림 전송 (공통 함수)"""
    import requests
    
    try:
        slack_token = os.environ.get('SLACK_BOT_TOKEN')
        if not slack_token:
            logger.warning("Slack 토큰이 설정되지 않았습니다.")
            return
        
        config = load_config()
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {slack_token}"},
            json={
                "channel": config['slack']['channel'],
                "text": message
            }
        )
        if response.status_code == 200:
            logger.info(f"Slack 메시지 전송 완료: {message[:50]}...")
        else:
            logger.error(f"Slack 메시지 전송 실패: {response.text}")
    except Exception as e:
        logger.error(f"Slack 메시지 전송 중 오류: {e}")


# =============================================================================
# 공통 DAG 팩토리 클래스
# =============================================================================

class CommonDAGFactory:
    """
    공통 DAG 생성 팩토리
    - 파라미터 기반 DAG 생성
    - 재사용성 및 확장성 제공
    """
    
    def __init__(self, dag_id: str, default_args: Optional[Dict] = None, 
                 schedule_interval: Optional[str] = None, 
                 catchup: bool = False,
                 tags: Optional[List[str]] = None):
        """
        공통 DAG 팩토리 초기화
        
        Args:
            dag_id: DAG 식별자
            default_args: 기본 인자
            schedule_interval: 스케줄 간격
            catchup: 과거 실행 여부
            tags: 태그 목록
        """
        self.dag_id = dag_id
        self.default_args = default_args or {
            'owner': 'airflow',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
        self.schedule_interval = schedule_interval
        self.catchup = catchup
        self.tags = tags or ['common', 'databricks']
        self.tasks = []
        
    def create_dag(self) -> DAG:
        """DAG 생성"""
        dag = DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
            catchup=self.catchup,
            tags=self.tags,
            max_active_runs=1,
        )
        return dag
    
    def add_task(self, task_id: str, python_callable: Callable, 
                 op_kwargs: Optional[Dict] = None,
                 task_group: Optional[TaskGroup] = None):
        """태스크 추가"""
        self.tasks.append({
            'task_id': task_id,
            'python_callable': python_callable,
            'op_kwargs': op_kwargs or {},
            'task_group': task_group
        })
        
    def build(self) -> DAG:
        """DAG 빌드"""
        dag = self.create_dag()
        
        with dag:
            start = EmptyOperator(task_id='start')
            end = EmptyOperator(task_id='end')
            
            previous_task = start
            for task_info in self.tasks:
                task = PythonOperator(
                    task_id=task_info['task_id'],
                    python_callable=task_info['python_callable'],
                    op_kwargs=task_info['op_kwargs'],
                    task_group=task_info['task_group']
                )
                previous_task >> task
                previous_task = task
                
            previous_task >> end
            
        return dag


# =============================================================================
# 공통 작업 함수들
# =============================================================================

def check_s3_chk_file(engine: str, schema: str, table: str, 
                      bucket: str = "databricks-test-lhk") -> bool:
    """
    S3 chk 파일 존재 여부 확인
    - rclone 복제 완료 신호 확인
    """
    import boto3
    
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
    """
    import time
    
    for attempt in range(max_retries):
        if check_s3_chk_file(engine, schema, table, bucket):
            return True
        logger.info(f"rclone 복제 대기 중... ({attempt + 1}/{max_retries})")
        time.sleep(retry_interval)
    
    raise Exception(f"rclone 복제 대기 시간 초과: {engine}/{schema}/{table}")


def execute_autoloader(engine: str, schema: str, table: str,
                       source_path: str, target_table: str,
                       file_format: str = "PARQUET",
                       options: Optional[Dict] = None) -> Dict:
    """
    Autoloader 실행
    - S3 데이터를 Databricks managed table에 적재
    """
    options = options or {}
    options_str = ", ".join([f"'{k}' = '{v}'" for k, v in options.items()])
    
    sql = f"""
        COPY INTO {target_table}
        FROM '{source_path}'
        FILEFORMAT = {file_format}
        COPY_OPTIONS ({options_str})
    """
    
    result = execute_databricks_sql(sql)
    logger.info(f"Autoloader 완료: {target_table}")
    
    return {
        "engine": engine,
        "schema": schema,
        "table": table,
        "target_table": target_table,
        "status": "success",
        "result": result
    }


def check_row_counts(source_table: str, target_table: str) -> Dict:
    """
    원본 및 대상 테이블 건수 체크
    """
    source_count = execute_databricks_sql(f"SELECT COUNT(*) as cnt FROM {source_table}")
    target_count = execute_databricks_sql(f"SELECT COUNT(*) as cnt FROM {target_table}")
    
    source_cnt = source_count['result']['data_array'][0][0]
    target_cnt = target_count['result']['data_array'][0][0]
    
    return {
        "source_count": source_cnt,
        "target_count": target_cnt,
        "match": source_cnt == target_cnt
    }


def save_etl_check_log(engine: str, schema: str, table: str,
                       source_count: int, target_count: int,
                       match: bool, job_id: str = ""):
    """ETL 체크 로그 저장"""
    log_table = "workspace.meta.etl_check_log"
    
    sql = f"""
        INSERT INTO {log_table} (job_id, engine, schema_name, table_name, 
                                  source_count, target_count, match_yn, created_at)
        VALUES ('{job_id}', '{engine}', '{schema}', '{table}',
                {source_count}, {target_count}, '{'Y' if match else 'N'}', 
                '{datetime.now().isoformat()}')
    """
    execute_databricks_sql(sql)


def update_profile_flag(engine: str, schema: str, table: str, 
                        flag: str = "Y"):
    """
    ETL 프로파일 활성화 플래그 업데이트
    - 초기 적재 완료 후 정규 작업 활성화
    """
    import yaml
    
    profile_path = os.path.join(BASE_DIR, "config", "profiles", 
                                f"{engine}_{schema}.yaml")
    
    if not os.path.exists(profile_path):
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
# 공통 DAG 템플릿 함수들
# =============================================================================

def create_autoloader_dag(dag_id: str, 
                          tables_config: List[Dict],
                          schedule_interval: Optional[str] = None) -> DAG:
    """
    Autoloader 공통 DAG 생성
    - 파라미터 기반 복수 테이블 처리
    - 재사용성 및 확장성 제공
    
    Args:
        dag_id: DAG 식별자
        tables_config: 테이블 설정 목록
            [
                {
                    "engine": "mysql",
                    "schema": "schema_name",
                    "table": "table_name",
                    "source_path": "s3://bucket/path",
                    "target_table": "catalog.schema.table",
                    "file_format": "PARQUET",
                    "options": {"mergeSchema": "true"}
                },
                ...
            ]
        schedule_interval: 스케줄 간격
    """
    
    def autoloader_task(engine: str, schema: str, table: str,
                        source_path: str, target_table: str,
                        file_format: str = "PARQUET",
                        options: Optional[Dict] = None,
                        **kwargs):
        """Autoloader 처리 태스크"""
        job_id = kwargs.get('dag_run').run_id if kwargs.get('dag_run') else "manual"
        
        # 1. rclone 복제 완료 대기
        wait_for_rclone_completion(engine, schema, table)
        
        # 2. Autoloader 실행
        result = execute_autoloader(engine, schema, table, 
                                    source_path, target_table, 
                                    file_format, options)
        
        # 3. 건수 체크
        count_result = check_row_counts(source_path, target_table)
        
        # 4. 로그 저장
        save_etl_check_log(engine, schema, table,
                          count_result['source_count'],
                          count_result['target_count'],
                          count_result['match'], job_id)
        
        # 5. 프로파일 플래그 업데이트
        update_profile_flag(engine, schema, table, "Y")
        
        # 6. Slack 알림
        send_slack_notification(
            f"[Autoloader 완료] {engine}.{schema}.{table} "
            f"(건수: {count_result['target_count']})"
        )
        
        return result
    
    factory = CommonDAGFactory(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        tags=['autoloader', 'databricks', 'common']
    )
    
    for idx, config in enumerate(tables_config):
        factory.add_task(
            task_id=f"autoloader_{config['engine']}_{config['schema']}_{config['table']}",
            python_callable=autoloader_task,
            op_kwargs=config
        )
    
    return factory.build()


def create_etl_dag(dag_id: str,
                   etl_configs: List[Dict],
                   schedule_interval: Optional[str] = None) -> DAG:
    """
    ETL 공통 DAG 생성
    - 파라미터 기반 복수 스키마/테이블 처리
    - 재사용성 및 확장성 제공
    
    Args:
        dag_id: DAG 식별자
        etl_configs: ETL 설정 목록
            [
                {
                    "engine": "mysql",
                    "schema": "schema_name",
                    "workers": 1
                },
                ...
            ]
        schedule_interval: 스케줄 간격
    """
    
    def etl_task(engine: str, schema: str, workers: int = 1, **kwargs):
        """ETL 처리 태스크"""
        import sys
        sys.path.insert(0, os.path.join(BASE_DIR, "etl-module"))
        
        from processor import process_schema
        
        job_id = kwargs.get('dag_run').run_id if kwargs.get('dag_run') else "manual"
        return process_schema(engine, schema, workers, job_id)
    
    factory = CommonDAGFactory(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        tags=['etl', 'databricks', 'common']
    )
    
    for config in etl_configs:
        factory.add_task(
            task_id=f"etl_{config['engine']}_{config['schema']}",
            python_callable=etl_task,
            op_kwargs=config
        )
    
    return factory.build()


# =============================================================================
# DAG 인스턴스 생성 (실제 사용 예시)
# =============================================================================

# Autoloader DAG 설정
AUTOLOADER_TABLES = [
    {
        "engine": "mysql",
        "schema": "mysql_schema_1",
        "table": "table_1",
        "source_path": "s3://databricks-test-lhk/mysql/mysql_schema_1/table_1",
        "target_table": "workspace.mysql_schema_1.table_1",
        "file_format": "PARQUET",
        "options": {"mergeSchema": "true"}
    },
    {
        "engine": "mysql",
        "schema": "mysql_schema_1",
        "table": "table_2",
        "source_path": "s3://databricks-test-lhk/mysql/mysql_schema_1/table_2",
        "target_table": "workspace.mysql_schema_1.table_2",
        "file_format": "PARQUET",
        "options": {"mergeSchema": "true"}
    },
    # 추가 테이블 설정 가능
]

# ETL DAG 설정
ETL_CONFIGS = [
    {
        "engine": "mysql",
        "schema": "mysql_schema_1",
        "workers": 1
    },
    {
        "engine": "mongodb",
        "schema": "mongo_schema_1",
        "workers": 1
    },
    {
        "engine": "postgresql",
        "schema": "pg_schema_1",
        "workers": 1
    },
]

# DAG 생성
autoloader_dag = create_autoloader_dag(
    dag_id="autoloader_common_dag",
    tables_config=AUTOLOADER_TABLES,
    schedule_interval=None  # 수동 트리거 또는 외부 스케줄러
)

etl_dag = create_etl_dag(
    dag_id="etl_common_dag",
    etl_configs=ETL_CONFIGS,
    schedule_interval=None
)
