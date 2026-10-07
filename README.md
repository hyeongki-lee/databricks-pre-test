# Databricks Pre-Test Environment

실제 운영환경을 가장한 미니 ETL 테스트 플랫폼

## 프로젝트 구조

```
databricks-pre-test/
├── airflow/              # Airflow DAG 정의
│   └── dags/            # 공통 DAG 및 작업 DAG
├── data-prep/           # 데이터 준비 스크립트
│   └── generate.py      # 12개 스키마, 60개 테이블 생성
├── rclone/              # rclone 설정
│   └── rclone.conf      # rcd API 설정
├── etl-dashboard/       # ETL 대시보드 (Streamlit)
│   └── app.py           # 대시보드 메인
├── etl-module/          # ETL 처리 모듈
│   └── processor.py     # YAML/JSON 기반 동적 ETL
├── autoloader/          # Autoloader 설정
│   └── config.py        # Autoloader 구성
├── config/              # 설정 파일
│   ├── connections.yaml # 엔진 접속 정보
│   └── profiles/        # ETL 프로파일 (YAML/JSON)
├── docs/                # 문서
│   └── manual.md        # 매뉴얼
└── scripts/             # 유틸리티 스크립트
    └── send_mail.py    # 메일 발송
```

## 실행 순서

1. Docker 실행: `cd C:\Users\lee21\lakehouse; docker compose up -d`
2. 데이터 준비: `python data-prep/generate.py`
3. rclone 복제: `rclone rc` API 호출
4. Autoloader 초기 이관: Airflow DAG 실행
5. ETL 모듈 실행: Airflow DAG 실행
6. 대시보드 확인: `streamlit run etl-dashboard/app.py`

## 주요 기능

- **ETL 시작 날짜**: 일/주/월/분기/반기/년 주기별 ETL 시작 날짜 관리
- **컬럼 추가/삭제 처리**: 대시보드에서 WEB 기반으로 수행
- **비식별화**: D1(SHA256), D2(SHA256 with salt), D3(Masking)
- **처리 방식**: append(증분), truncate(전체재적재), merge(변동분)
- **검증**: 건수 체크, 작업 기록 테이블
- **Slack 알림**: 컬럼 변경, 작업 완료/실패 알림

## GitHub

- 저장소: https://github.com/hyeongki-lee/databricks-pre-test
