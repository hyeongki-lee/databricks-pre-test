"""
ETL 대시보드
- 엔진별 접속정보 관리
- 스키마별 YAML 프로파일 관리
- ETL 프로파일 빌더
- 컬럼 관리 (추가/삭제)
- 비식별화 설정 (D1, D2, D3)
- 스케줄 설정
- ETL 실행 및 통계 조회
"""

import os
import sys
import json
import yaml
import logging
import hashlib
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from functools import wraps

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash

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
ETL_MODULE_PATH = BASE_DIR / "etl-module" / "processor.py"

# Flask 앱 설정
app = Flask(__name__)
app.secret_key = 'etl-dashboard-secret-key-2026'

# 코드 정의
DEIDENTIFICATION_CODES = {
    'D1': 'Hash (SHA256)',
    'D2': 'Null 처리',
    'D3': 'Masking'
}

ETL_TYPES = {
    'append': 'Append (증분)',
    'truncate': 'Truncate (전체재적재)',
    'merge': 'Merge (변동분, PK필수)'
}

SCHEDULE_TYPES = {
    'daily': '일간',
    'weekly': '주간',
    'monthly': '월간',
    'quarterly': '분기',
    'semi_annually': '반기',
    'annually': '년간'
}

DAY_OF_WEEK = {
    'monday': '월요일',
    'tuesday': '화요일',
    'wednesday': '수요일',
    'thursday': '목요일',
    'friday': '금요일',
    'saturday': '토요일',
    'sunday': '일요일'
}


def load_config():
    """설정 파일 로드"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def save_config(config: Dict):
    """설정 파일 저장"""
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)


def load_profile(engine: str, schema: str) -> Optional[Dict]:
    """ETL 프로파일 로드"""
    profile_path = PROFILES_DIR / f"{engine}_{schema}.yaml"
    if not profile_path.exists():
        return None
    with open(profile_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def save_profile(engine: str, schema: str, profile: Dict):
    """ETL 프로파일 저장"""
    PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    profile_path = PROFILES_DIR / f"{engine}_{schema}.yaml"
    with open(profile_path, 'w', encoding='utf-8') as f:
        yaml.dump(profile, f, allow_unicode=True, default_flow_style=False)


def get_all_profiles() -> List[Dict]:
    """모든 프로파일 조회"""
    profiles = []
    if not PROFILES_DIR.exists():
        return profiles
    
    for profile_file in PROFILES_DIR.glob("*.yaml"):
        parts = profile_file.stem.split('_', 1)
        if len(parts) == 2:
            engine, schema = parts
            profile = load_profile(engine, schema)
            if profile:
                profile['_engine'] = engine
                profile['_schema'] = schema
                profiles.append(profile)
    
    return profiles


def get_source_tables(engine: str, schema: str) -> List[str]:
    """소스 엔진에서 테이블 목록 조회"""
    config = load_config()
    
    try:
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
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables

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
            tables = db.list_collection_names()
            client.close()
            return tables

        elif engine == "postgresql":
            import psycopg2
            conn = psycopg2.connect(
                host=config['postgresql']['host'],
                port=config['postgresql']['port'],
                user=config['postgresql']['user'],
                password=config['postgresql']['password'],
                dbname=schema
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables

    except Exception as e:
        logger.error(f"테이블 목록 조회 실패: {engine}.{schema} - {e}")
        return []


def get_source_columns(engine: str, schema: str, table: str) -> List[Dict]:
    """소스 테이블 컬럼 목록 조회"""
    config = load_config()
    columns = []
    
    try:
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
            cursor.execute(f"DESCRIBE {table}")
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'key': row[3],
                    'default': row[4]
                })
            cursor.close()
            conn.close()

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
            first_doc = collection.find_one()
            if first_doc:
                for key, value in first_doc.items():
                    if key != '_id':
                        columns.append({
                            'name': key,
                            'type': type(value).__name__,
                            'nullable': True,
                            'key': '',
                            'default': None
                        })
            client.close()

        elif engine == "postgresql":
            import psycopg2
            conn = psycopg2.connect(
                host=config['postgresql']['host'],
                port=config['postgresql']['port'],
                user=config['postgresql']['user'],
                password=config['postgresql']['password'],
                dbname=schema
            )
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{table}'
                ORDER BY ordinal_position
            """)
            for row in cursor.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'nullable': row[2] == 'YES',
                    'key': '',
                    'default': row[3]
                })
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"컬럼 목록 조회 실패: {engine}.{schema}.{table} - {e}")
    
    return columns


def get_databricks_stats() -> Dict:
    """Databricks ETL 통계 조회"""
    stats = {
        'total_jobs': 0,
        'success_jobs': 0,
        'failed_jobs': 0,
        'skipped_jobs': 0,
        'total_source_count': 0,
        'total_target_count': 0,
        'match_count': 0,
        'mismatch_count': 0,
        'recent_runs': []
    }
    
    try:
        token = os.environ.get('DATABRICKS_TOKEN')
        if not token:
            return stats
        
        import requests
        config = load_config()
        host = config.get('databricks', {}).get('host', '')
        warehouse_id = config.get('databricks', {}).get('warehouse_id', '')
        
        # 최근 실행 로그 조회
        response = requests.post(
            f"{host}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "warehouse_id": warehouse_id,
                "statement": """
                    SELECT 
                        job_id, engine, schema_name, table_name, etl_type, 
                        status, message, created_at
                    FROM workspace.meta.etl_run_log
                    ORDER BY created_at DESC
                    LIMIT 20
                """,
                "wait_timeout": "30s"
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'result' in result and 'data_array' in result['result']:
                stats['recent_runs'] = result['result']['data_array']
        
        # 통계 조회
        response = requests.post(
            f"{host}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "warehouse_id": warehouse_id,
                "statement": """
                    SELECT 
                        COUNT(*) as total_jobs,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_jobs,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_jobs,
                        SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as skipped_jobs,
                        SUM(source_count) as total_source_count,
                        SUM(target_count) as total_target_count,
                        SUM(CASE WHEN match_yn = 'Y' THEN 1 ELSE 0 END) as match_count,
                        SUM(CASE WHEN match_yn = 'N' THEN 1 ELSE 0 END) as mismatch_count
                    FROM workspace.meta.etl_check_log
                """,
                "wait_timeout": "30s"
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'result' in result and 'data_array' in result['result']:
                data = result['result']['data_array'][0]
                stats['total_jobs'] = data[0] or 0
                stats['success_jobs'] = data[1] or 0
                stats['failed_jobs'] = data[2] or 0
                stats['skipped_jobs'] = data[3] or 0
                stats['total_source_count'] = data[4] or 0
                stats['total_target_count'] = data[5] or 0
                stats['match_count'] = data[6] or 0
                stats['mismatch_count'] = data[7] or 0
    
    except Exception as e:
        logger.error(f"Databricks 통계 조회 실패: {e}")
    
    return stats


def run_etl(engine: str, schema: str, workers: int = 1) -> Dict:
    """ETL 실행"""
    try:
        cmd = [
            sys.executable,
            str(ETL_MODULE_PATH),
            '--engine', engine,
            '--schema', schema,
            '--workers', str(workers),
            '--job-id', f"ETL_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode
        }
    
    except Exception as e:
        return {
            'success': False,
            'stdout': '',
            'stderr': str(e),
            'returncode': -1
        }


# 라우트 정의
@app.route('/')
def index():
    """메인 대시보드"""
    config = load_config()
    profiles = get_all_profiles()
    stats = get_databricks_stats()
    
    return render_template('index.html', 
                         config=config,
                         profiles=profiles,
                         stats=stats,
                         deidentification_codes=DEIDENTIFICATION_CODES,
                         etl_types=ETL_TYPES,
                         schedule_types=SCHEDULE_TYPES)


@app.route('/profiles')
def list_profiles():
    """프로파일 목록"""
    profiles = get_all_profiles()
    return render_template('profiles.html', 
                         profiles=profiles,
                         deidentification_codes=DEIDENTIFICATION_CODES,
                         etl_types=ETL_TYPES,
                         schedule_types=SCHEDULE_TYPES)


@app.route('/profile/<engine>/<schema>')
def view_profile(engine: str, schema: str):
    """프로파일 상세 조회"""
    profile = load_profile(engine, schema)
    if not profile:
        flash(f"프로파일을 찾을 수 없습니다: {engine}.{schema}", 'error')
        return redirect(url_for('list_profiles'))
    
    return render_template('profile_detail.html',
                         engine=engine,
                         schema=schema,
                         profile=profile,
                         deidentification_codes=DEIDENTIFICATION_CODES,
                         etl_types=ETL_TYPES,
                         schedule_types=SCHEDULE_TYPES,
                         day_of_week=DAY_OF_WEEK)


@app.route('/profile/<engine>/<schema>/edit', methods=['GET', 'POST'])
def edit_profile(engine: str, schema: str):
    """프로파일 편집"""
    profile = load_profile(engine, schema)
    if not profile:
        flash(f"프로파일을 찾을 수 없습니다: {engine}.{schema}", 'error')
        return redirect(url_for('list_profiles'))
    
    if request.method == 'POST':
        # 프로파일 업데이트
        profile['schedule'] = {
            'type': request.form.get('schedule_type', 'monthly'),
            'start_date': request.form.get('start_date', ''),
            'start_day': int(request.form.get('start_day', 1)),
            'day_of_week': request.form.get('day_of_week', 'monday'),
            'day_of_month': int(request.form.get('day_of_month', 1))
        }
        
        # 테이블 설정 업데이트
        tables = profile.get('tables', {})
        for table_name in tables.keys():
            tables[table_name]['etl_type'] = request.form.get(f'etl_type_{table_name}', 'append')
            
            # 비식별화 설정
            deidentification = {}
            for col in tables[table_name].get('columns', []):
                deid_method = request.form.get(f'deid_{table_name}_{col}', '')
                if deid_method:
                    deidentification[col] = deid_method
            tables[table_name]['deidentification'] = deidentification
            
            # exclude_columns
            exclude_columns = request.form.getlist(f'exclude_{table_name}')
            tables[table_name]['exclude_columns'] = exclude_columns
            
            # merge인 경우 pk_columns
            if tables[table_name]['etl_type'] == 'merge':
                pk_columns = request.form.get(f'pk_{table_name}', '')
                tables[table_name]['pk_columns'] = [c.strip() for c in pk_columns.split(',') if c.strip()]
        
        save_profile(engine, schema, profile)
        flash(f"프로파일이 업데이트되었습니다: {engine}.{schema}", 'success')
        return redirect(url_for('view_profile', engine=engine, schema=schema))
    
    return render_template('profile_edit.html',
                         engine=engine,
                         schema=schema,
                         profile=profile,
                         deidentification_codes=DEIDENTIFICATION_CODES,
                         etl_types=ETL_TYPES,
                         schedule_types=SCHEDULE_TYPES,
                         day_of_week=DAY_OF_WEEK)


@app.route('/profile/<engine>/<schema>/table/add', methods=['GET', 'POST'])
def add_table(engine: str, schema: str):
    """신규 테이블 추가"""
    profile = load_profile(engine, schema)
    if not profile:
        flash(f"프로파일을 찾을 수 없습니다: {engine}.{schema}", 'error')
        return redirect(url_for('list_profiles'))
    
    if request.method == 'POST':
        table_name = request.form.get('table_name', '').strip()
        if not table_name:
            flash("테이블명을 입력해주세요.", 'error')
            return redirect(url_for('add_table', engine=engine, schema=schema))
        
        # 소스 컬럼 조회
        source_columns = get_source_columns(engine, schema, table_name)
        if not source_columns:
            flash(f"소스 테이블을 찾을 수 없습니다: {table_name}", 'error')
            return redirect(url_for('add_table', engine=engine, schema=schema))
        
        # 테이블 설정 추가
        if 'tables' not in profile:
            profile['tables'] = {}
        
        profile['tables'][table_name] = {
            'columns': [col['name'] for col in source_columns],
            'exclude_columns': [],
            'deidentification': {},
            'etl_type': request.form.get('etl_type', 'append'),
            'pk_columns': []
        }
        
        save_profile(engine, schema, profile)
        flash(f"테이블이 추가되었습니다: {table_name}", 'success')
        return redirect(url_for('view_profile', engine=engine, schema=schema))
    
    # 소스 테이블 목록
    source_tables = get_source_tables(engine, schema)
    existing_tables = list(profile.get('tables', {}).keys())
    available_tables = [t for t in source_tables if t not in existing_tables]
    
    return render_template('table_add.html',
                         engine=engine,
                         schema=schema,
                         available_tables=available_tables,
                         etl_types=ETL_TYPES)


@app.route('/profile/<engine>/<schema>/table/<table_name>/columns', methods=['GET', 'POST'])
def manage_columns(engine: str, schema: str, table_name: str):
    """컬럼 관리 (추가/삭제)"""
    profile = load_profile(engine, schema)
    if not profile or table_name not in profile.get('tables', {}):
        flash(f"테이블을 찾을 수 없습니다: {table_name}", 'error')
        return redirect(url_for('view_profile', engine=engine, schema=schema))
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'add':
            col_name = request.form.get('column_name', '').strip()
            col_type = request.form.get('column_type', 'string')
            
            if col_name:
                table_config = profile['tables'][table_name]
                if col_name not in table_config['columns']:
                    table_config['columns'].append(col_name)
                    # 소스 컬럼 타입 정보 업데이트
                    source_columns = get_source_columns(engine, schema, table_name)
                    for col in source_columns:
                        if col['name'] == col_name:
                            table_config.setdefault('column_types', {})[col_name] = col['type']
                            break
                    save_profile(engine, schema, profile)
                    flash(f"컬럼이 추가되었습니다: {col_name}", 'success')
        
        elif action == 'delete':
            col_name = request.form.get('column_name', '').strip()
            if col_name:
                table_config = profile['tables'][table_name]
                if col_name in table_config['columns']:
                    table_config['columns'].remove(col_name)
                    if col_name in table_config.get('exclude_columns', []):
                        table_config['exclude_columns'].remove(col_name)
                    if col_name in table_config.get('deidentification', {}):
                        del table_config['deidentification'][col_name]
                    save_profile(engine, schema, profile)
                    flash(f"컬럼이 삭제되었습니다: {col_name}", 'success')
        
        elif action == 'sync':
            # 소스 컬럼 동기화
            source_columns = get_source_columns(engine, schema, table_name)
            source_col_names = [col['name'] for col in source_columns]
            
            table_config = profile['tables'][table_name]
            current_cols = table_config['columns']
            
            # 추가된 컬럼
            added = [c for c in source_col_names if c not in current_cols]
            # 삭제된 컬럼
            removed = [c for c in current_cols if c not in source_col_names]
            
            table_config['columns'] = source_col_names
            
            # 삭제된 컬럼 정리
            for col in removed:
                if col in table_config.get('exclude_columns', []):
                    table_config['exclude_columns'].remove(col)
                if col in table_config.get('deidentification', {}):
                    del table_config['deidentification'][col]
            
            save_profile(engine, schema, profile)
            flash(f"컬럼 동기화 완료: 추가 {len(added)}개, 삭제 {len(removed)}개", 'success')
        
        return redirect(url_for('manage_columns', engine=engine, schema=schema, table_name=table_name))
    
    table_config = profile['tables'][table_name]
    source_columns = get_source_columns(engine, schema, table_name)
    
    return render_template('columns.html',
                         engine=engine,
                         schema=schema,
                         table_name=table_name,
                         table_config=table_config,
                         source_columns=source_columns,
                         deidentification_codes=DEIDENTIFICATION_CODES)


@app.route('/profile/<engine>/<schema>/table/<table_name>/delete', methods=['POST'])
def delete_table(engine: str, schema: str, table_name: str):
    """테이블 삭제"""
    profile = load_profile(engine, schema)
    if not profile or table_name not in profile.get('tables', {}):
        flash(f"테이블을 찾을 수 없습니다: {table_name}", 'error')
        return redirect(url_for('view_profile', engine=engine, schema=schema))
    
    del profile['tables'][table_name]
    save_profile(engine, schema, profile)
    flash(f"테이블이 삭제되었습니다: {table_name}", 'success')
    return redirect(url_for('view_profile', engine=engine, schema=schema))


@app.route('/etl/run', methods=['POST'])
def execute_etl():
    """ETL 실행"""
    engine = request.form.get('engine')
    schema = request.form.get('schema')
    workers = int(request.form.get('workers', 1))
    
    if not engine or not schema:
        flash("엔진과 스키마를 선택해주세요.", 'error')
        return redirect(url_for('index'))
    
    result = run_etl(engine, schema, workers)
    
    if result['success']:
        flash(f"ETL 실행이 완료되었습니다: {engine}.{schema}", 'success')
    else:
        flash(f"ETL 실행 실패: {result['stderr']}", 'error')
    
    return redirect(url_for('index'))


@app.route('/api/stats')
def api_stats():
    """API: 통계 조회"""
    return jsonify(get_databricks_stats())


@app.route('/api/profiles')
def api_profiles():
    """API: 프로파일 목록"""
    return jsonify(get_all_profiles())


@app.route('/api/source/tables')
def api_source_tables():
    """API: 소스 테이블 목록"""
    engine = request.args.get('engine')
    schema = request.args.get('schema')
    
    if not engine or not schema:
        return jsonify({'error': 'engine and schema are required'}), 400
    
    tables = get_source_tables(engine, schema)
    return jsonify({'tables': tables})


@app.route('/api/source/columns')
def api_source_columns():
    """API: 소스 컬럼 목록"""
    engine = request.args.get('engine')
    schema = request.args.get('schema')
    table = request.args.get('table')
    
    if not engine or not schema or not table:
        return jsonify({'error': 'engine, schema, and table are required'}), 400
    
    columns = get_source_columns(engine, schema, table)
    return jsonify({'columns': columns})


@app.route('/builder')
def etl_builder():
    """ETL 빌더 (신규 스키마/테이블 등록)"""
    config = load_config()
    return render_template('builder.html',
                         config=config,
                         deidentification_codes=DEIDENTIFICATION_CODES,
                         etl_types=ETL_TYPES,
                         schedule_types=SCHEDULE_TYPES,
                         day_of_week=DAY_OF_WEEK)


@app.route('/builder/create', methods=['POST'])
def builder_create():
    """ETL 빌더: 신규 프로파일 생성"""
    engine = request.form.get('engine')
    schema = request.form.get('schema')
    table = request.form.get('table')
    
    if not engine or not schema or not table:
        flash("엔진, 스키마, 테이블을 모두 선택해주세요.", 'error')
        return redirect(url_for('etl_builder'))
    
    # 소스 컬럼 조회
    source_columns = get_source_columns(engine, schema, table)
    if not source_columns:
        flash(f"소스 테이블을 찾을 수 없습니다: {table}", 'error')
        return redirect(url_for('etl_builder'))
    
    # 프로파일 로드 또는 생성
    profile = load_profile(engine, schema)
    if not profile:
        profile = {
            'engine': engine,
            'schema': schema,
            'schedule': {
                'type': request.form.get('schedule_type', 'monthly'),
                'start_date': request.form.get('start_date', ''),
                'start_day': int(request.form.get('start_day', 1)),
                'day_of_week': request.form.get('day_of_week', 'monday'),
                'day_of_month': int(request.form.get('day_of_month', 1))
            },
            'tables': {}
        }
    
    # 테이블 설정
    deidentification = {}
    for col in source_columns:
        deid_method = request.form.get(f'deid_{col["name"]}', '')
        if deid_method:
            deidentification[col['name']] = deid_method
    
    exclude_columns = request.form.getlist('exclude_columns')
    
    etl_type = request.form.get('etl_type', 'append')
    pk_columns = []
    if etl_type == 'merge':
        pk_columns_str = request.form.get('pk_columns', '')
        pk_columns = [c.strip() for c in pk_columns_str.split(',') if c.strip()]
    
    profile['tables'][table] = {
        'columns': [col['name'] for col in source_columns],
        'exclude_columns': exclude_columns,
        'deidentification': deidentification,
        'etl_type': etl_type,
        'pk_columns': pk_columns
    }
    
    save_profile(engine, schema, profile)
    flash(f"ETL 프로파일이 생성/업데이트되었습니다: {engine}.{schema}.{table}", 'success')
    return redirect(url_for('view_profile', engine=engine, schema=schema))


def init_profiles():
    """초기 프로파일 생성 (4개 스키마, 5개 테이블)"""
    config = load_config()
    
    # 엔진별 스키마 목록
    engines = {
        'mysql': config.get('mysql', {}).get('schemas', []),
        'mongodb': config.get('mongodb', {}).get('schemas', []),
        'postgresql': config.get('postgresql', {}).get('schemas', [])
    }
    
    # 각 엔진별 5개 테이블 생성
    default_tables = ['users', 'orders', 'products', 'categories', 'logs']
    
    # 비식별화 분배를 위한 코드 목록
    deid_codes = ['D1', 'D2', 'D3']
    
    for engine, schemas in engines.items():
        for schema_info in schemas:
            schema_name = schema_info['name']
            profile_path = PROFILES_DIR / f"{engine}_{schema_name}.yaml"
            
            if profile_path.exists():
                continue
            
            # 스키마별 프로파일 생성
            profile = {
                'engine': engine,
                'schema': schema_name,
                'schedule': {
                    'type': 'monthly',
                    'start_date': '2026-01-01',
                    'start_day': 1,
                    'day_of_week': 'monday',
                    'day_of_month': 1
                },
                'tables': {}
            }
            
            # 테이블별 설정
            for idx, table in enumerate(default_tables):
                # 소스 컬럼 조회
                source_columns = get_source_columns(engine, schema_name, table)
                if not source_columns:
                    # 소스가 없으면 기본 컬럼 생성
                    source_columns = [
                        {'name': 'id', 'type': 'int', 'nullable': False, 'key': 'PRI', 'default': None},
                        {'name': 'name', 'type': 'varchar', 'nullable': True, 'key': '', 'default': None},
                        {'name': 'email', 'type': 'varchar', 'nullable': True, 'key': '', 'default': None},
                        {'name': 'created_at', 'type': 'datetime', 'nullable': True, 'key': '', 'default': None},
                        {'name': 'updated_at', 'type': 'datetime', 'nullable': True, 'key': '', 'default': None}
                    ]
                
                # 비식별화 설정 (각 엔진별로 골고루 분배)
                deidentification = {}
                for col in source_columns:
                    # 이메일 컬럼에 대해 비식별화 적용
                    if 'email' in col['name'].lower():
                        deidentification[col['name']] = deid_codes[idx % 3]
                    # 이름 컬럼에 대해 비식별화 적용
                    elif 'name' in col['name'].lower():
                        deidentification[col['name']] = deid_codes[(idx + 1) % 3]
                
                # ETL 타입 설정 (다양하게 분배)
                etl_types = ['append', 'truncate', 'merge']
                etl_type = etl_types[idx % 3]
                
                # merge인 경우 pk_columns 설정
                pk_columns = []
                if etl_type == 'merge':
                    pk_columns = ['id']
                
                profile['tables'][table] = {
                    'columns': [col['name'] for col in source_columns],
                    'exclude_columns': [],
                    'deidentification': deidentification,
                    'etl_type': etl_type,
                    'pk_columns': pk_columns
                }
            
            save_profile(engine, schema_name, profile)
            logger.info(f"초기 프로파일 생성: {engine}.{schema_name}")


@app.cli.command('init')
def init_command():
    """초기 프로파일 생성 명령"""
    init_profiles()
    print("초기 프로파일 생성 완료!")


if __name__ == '__main__':
    # 초기 프로파일 자동 생성
    init_profiles()
    
    # 템플릿 폴더 생성
    template_dir = Path(__file__).parent / "templates"
    template_dir.mkdir(exist_ok=True)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
