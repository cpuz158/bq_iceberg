# BigQuery & Apache Iceberg 엔터프라이즈 성능 벤치마크 에이전트 가이드라인 (`GEMINI.md`)

## 🤖 에이전트 역할 및 미션 (Role & Mission)
> **너는 고도의 자율성을 가진 소프트웨어 엔지니어 에이전트이다.**
> **BigQuery Native Table 및 Apache Iceberg 3종 아키텍처 연동부터 동적 용량 조절(`TARGET_GB`), 무결성 검증, 벤치마킹 쿼리 실행, 4개 차원 시각화, 파일 레이아웃 심층 분석까지 완전 자동화된 주피터 노트북(`bq_iceberg_benchmark_50gb.ipynb`)을 100% 재현 및 생성할 수 있는 코드 레벨 프롬프트를 준수하라.**

---

## 🎯 1. 기본 인프라 & 환경 설정 규칙 (Environment Rules)

1. **GCP 리소스 규격**:
   - GCP Project ID: `USER_PROJECT_ID` (자동 감지 또는 사용자가 입력한 GCP Project ID)
   - BigQuery Dataset Location: `asia-northeast3` (서울 리전)
   - BigQuery Dataset Name: `bq_iceberg_benchmark_ds`
   - Connection Name: `lakehouse-iceberg-conn`
   - GCS Bucket Name: `{PROJECT_ID}-bq-iceberg-demo-bucket` (기본), `{PROJECT_ID}-bq-iceberg-dml-bucket` (DML/Maintenance)
2. **패키지 및 런타임 호환 규격**:
   - `pyspark>=3.5.0,<4.0.0` (Apache Iceberg Spark 3.5 Runtime 호환 버전 준수)
   - `pyiceberg[gcsfs]>=0.11.0` (GCS 오픈 메타데이터 엔진)
   - JDK 17+/26+ 호환성을 위한 JVM 모듈 개방 플래그 필수 주입:
     ```python
     import os
     os.environ["JDK_JAVA_OPTIONS"] = (
         "--add-opens=java.base/java.lang=ALL-UNNAMED "
         "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
         "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
         "--add-opens=java.base/java.io=ALL-UNNAMED "
         "--add-opens=java.base/java.net=ALL-UNNAMED "
         "--add-opens=java.base/java.nio=ALL-UNNAMED "
         "--add-opens=java.base/java.util=ALL-UNNAMED "
         "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
         "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
         "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
         "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
         "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
     )
     ```
3. **노트북 산출물 명세**:
   - 메인 대용량 가변 스케일 벤치마크 노트북: `bq_iceberg_benchmark_50gb.ipynb`
   - BigQuery Direct DML 검증 노트북: `bq_iceberg_bigquery_dml.ipynb`
   - Spark SQL 통합 쿼리 가이드 노트북: `bq_iceberg_spark_sql.ipynb`
   - Spark SQL DML Maintenance 노트북: `bq_iceberg_spark_sql_dml.ipynb` (`rewrite_data_files`, `expire_snapshots` 실행 포함)

---

## 💡 2. 동적 용량 조절 변수 (`TARGET_GB`) 및 시드 데이터 확장 규격

- **사용자 데이터 용량 설정 변수**:
  ```python
  TARGET_GB = 50  # 사용자가 1GB, 10GB, 50GB 등 자유롭게 지정
  ROWS_PER_GB = 3_000_000  # 7개 컬럼 스키마 기준 1GB당 약 300만 건
  TARGET_ROWS = TARGET_GB * ROWS_PER_GB
  SEED_ROWS = 1_000_000  # 시드 더미 데이터 (100만 건)
  MULTIPLIER = max(1, TARGET_ROWS // SEED_ROWS)  # 시드 데이터 확장 배율
  ```
- **시드 데이터 업로드**:
  - 100만 건 시드 파키 파일(`raw_data/chunk_*.parquet`)을 GCS 버킷에 먼저 업로드 후 Native Table 적재 시 `CROSS JOIN UNNEST(GENERATE_ARRAY(1, MULTIPLIER))` 로 목표 용량 병렬 확장.

---

## 📊 3. 4대 비교 대상 테이블 1:1 독립 셀 구축 규격

노트북 3단계에서는 아래 4개 비교 대상 테이블을 **독립된 개별 전용 코드 셀**로 명확히 분리하여 생성한다:

1. **`[3-2단계] Native Table (Capacitor)`**:
   - Table ID: `f"{PROJECT_ID}.{DATASET_NAME}.native_weblog"`
   - `PARTITION BY event_date`, `CLUSTER BY user_id, event_type`
2. **`[3-3단계] BQ Managed Iceberg Table`**:
   - Table ID: `f"{PROJECT_ID}.{DATASET_NAME}.managed_iceberg_weblog"`
   - `OPTIONS (file_format = 'PARQUET', table_format = 'ICEBERG')`
3. **`[3-4단계] Lakehouse Iceberg Table` (핵심 카탈로그 연동)**:
   - Table ID: `f"{PROJECT_ID}.{BUCKET_NAME}.default.external_weblog"`
   - GCP BigLake REST Catalog (`https://biglake.googleapis.com/iceberg/v1/restcatalog`) 연동
   - PyIceberg Fast Bulk Append API로 적재 (`write.sort.order = "user_id ASC NULLS FIRST, event_type ASC NULLS FIRST"`)
4. **`[3-5단계] External Iceberg Table` (정적 DDL 연동)**:
   - Table ID: `f"{PROJECT_ID}.{DATASET_NAME}.external_iceberg_weblog"`
   - `CREATE OR REPLACE EXTERNAL TABLE` DDL 및 GCS `metadata.json` URI 연결

---

## 📊 4. 데이터 무결성 검증 및 벤치마크 쿼리 시나리오 규격

### 4.1 실시간 SQL 무결성 검증 (4단계)
- 외부 REST Table metadata 0 반환 문제를 회피하기 위해, 4개 테이블 구축 완료 후 실시간 SQL **`SELECT COUNT(*)`** 쿼리를 실행하여 `TARGET_ROWS`와의 일치 여부를 pandas DataFrame 표로 검증.

### 4.2 3대 핵심 벤치마크 시나리오 (5단계)
- **쿼리 캐시 완전 비활성화**: `job_config = bigquery.QueryJobConfig(use_query_cache=False)`
- **시나리오 1: Partition Pruning** (`WHERE event_date BETWEEN '2026-07-03' AND '2026-07-05'`)
- **시나리오 2: Predicate Pushdown** (`WHERE user_id = 'USER_10500' AND event_type = 'PURCHASE'`)
- **시나리오 3: Full Scan Aggregation** (`GROUP BY device_os, event_type`)

### 4.3 4개 지표 고해상도 시각화 (6단계)
- `Bytes Processed (MB)`, `Elapsed Time (sec)`, `Slot Milliseconds (ms)`, `Estimated Query Cost ($ USD @ $6.25/TB)` 4개 서브플롯을 Seaborn으로 작성하고 DPI 300 차트(`benchmark_summary_visualization.png`)로 저장.

---

## 📌 5. 심층 분석 & 파일 레이아웃 Best Practice 요약 규격 (7단계)

노트북 최하단 마크다운 셀에는 다음 내용이 포함되어야 한다:
1. **BigQuery Native Capacitor vs Apache Iceberg Parquet 읽기 메커니즘 차이** (Direct Vectorized Read vs Parquet Footer Decoding).
2. **Parquet File Size 최적 권장값**: **256MB** (128MB ~ 512MB 범위).
3. **Row Group Size 최적 권장값**: **64MB ~ 128MB**.
4. **Small Files & Manifest 관리**: `write.manifest.target-size-bytes = 8MB`, Snapshot Expire 설정.
5. **Compaction 자동화 트리거 기준**: 소형 파일(<32MB) 수량 1,000개 이상 초과 시 또는 MoR Delete 파일 비율 > 15% 시 Spark `rewrite_data_files` 실행.
6. **Column Pruning & Row-Group Skipping (99.9% 스킵)**: `write.sort.order` 지정 및 PyArrow 물리 정렬 적재로 min/max 범위 수축.
7. **엔터프라이즈 Best Practice 요약 매트릭스 표**.
