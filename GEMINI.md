# BigQuery & Apache Iceberg 엔터프라이즈 성능 벤치마크 가이드라인 (`GEMINI.md`)

## 🤖 에이전트 역할 및 미션 (Role & Mission)
> **너는 고도의 자율성을 가진 소프트웨어 엔지니어 에이전트이다.**
> **BigQuery와 Apache Iceberg 연동 및 성능 최적화에 대한 코드 레벨 분석과 초기 환경 구축부터 벤치마킹까지 완전 자동화된 주피터 노트북(Jupyter Notebook)을 설계하라.**
>
> 본 지침서만으로도 50GB (1.5억 건) 규모의 BigQuery Native 및 Apache Iceberg (Managed, External, Metastore) 벤치마킹 파이프라인과 주피터 노트북 코드를 100% 재현 및 생성할 수 있도록 명확한 코드 레벨 표준을 정의한다.

---

## 🎯 1. 기본 인프라 & 환경 설정 규칙 (Environment Rules)

1. **프로젝트 & 데이터셋 규격**:
   - GCP Project ID: `your-gcp-project-id`
   - BigQuery Dataset Location: `asia-northeast3` (서울 리전)
   - Connection Name: `lakehouse-iceberg-conn`
2. **패키지 및 런타임 호환 규격**:
   - `pyspark>=4.2.0` (PySpark 최신버전 준수)
   - `pyiceberg[gcsfs]>=0.11.0` (GCS 오픈 메타데이터 엔진)
   - JDK 17+/26+ 호환성을 위한 JVM 모듈 개방 플래그 주입 필수:
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
3. **산출물 파일 배치 제약**:
   - 모든 산출물은 프로젝트 루트(`./`) 경로에 위치해야 함.
   - 메인 노트북: `bq_iceberg_deepdive.ipynb`
   - 심층 분석 전용 노트북: `bq_iceberg_analysis.ipynb`
   - 종합 기술 보고서: `bq_iceberg_deepdive_report.md`

---

## 📊 2. 4대 비교 테이블 & 데이터 적재 파이프라인 규격

### 2.1 4대 벤치마킹 대상 테이블
1. **`native_weblog`**: BigQuery Native Table (Capacitor 포맷)
2. **`managed_iceberg_weblog`**: BigQuery Managed Iceberg Table (스토리지 & 메타데이터 자동 관리)
3. **`external_iceberg_weblog`**: LakeHouse External Iceberg Table (PyIceberg & GCS 오픈 스토리지)
4. **`metastore_iceberg_weblog`**: BigQuery Metastore Iceberg Table (PySpark / Metastore API)

### 2.2 대용량 데이터 동기화 및 적재 규칙 (Data Scaling Rules)
- **정확히 150,000,000건 (1.5억 건, ~50GB)** 데이터 세트를 4대 테이블 전체에 일치시킬 것.
- **실시간 SQL 검증**: External Table REST API 메타데이터 (`num_rows`)가 0을 반환하므로, 실시간 SQL **`SELECT COUNT(*)`** 쿼리 실행 결과로 150,000,000 건을 검증할 것.
- **PyIceberg Fast Bulk Append 패턴**:
  - DML 제한 (`400 DML statements are only supported over tables stored in BigQuery`) 회피를 위해 PyIceberg Fast Append API 사용.
  - 1,000,000건 시드 데이터프레임을 150배 확장하여 날짜별 1,500만 건 단위로 단 10회의 Bulk Append로 수십 초 만에 적재 완료.

---

## ⚡ 3. 파티셔닝 & 클러스터 정렬 레이아웃 코드 레벨 규격

### 3.1 Partition Pruning (`event_date` 파티셔닝)
- **PyIceberg `PartitionSpec` 적용**:
  ```python
  from pyiceberg.partitioning import PartitionSpec, PartitionField
  from pyiceberg.transforms import IdentityTransform

  partition_spec = PartitionSpec(
      PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="event_date")
  )
  ```
- **GCS 스토리지 경로**: `data/event_date=YYYY-MM-DD/` 구조로 명시적 분할 적재.

### 3.2 Predicate Pushdown & Clustering (클러스터 정렬 구현)
- **BigQuery `CLUSTER BY` 와 100% 동일한 효과 구현**:
  ```python
  # 1. Iceberg Table Property 부여
  iceberg_table = catalog.create_table(
      identifier="default.external_weblog",
      schema=schema,
      partition_spec=partition_spec,
      properties={"write.sort.order": "user_id ASC NULLS FIRST, event_type ASC NULLS FIRST"}
  )

  # 2. PyArrow 적재 시 user_id, event_type 물리 정렬 및 Index 차단
  sorted_sub_df = sub_df.sort_values(by=['user_id', 'event_type']).reset_index(drop=True)
  pa_sub_table = pa.Table.from_pandas(sorted_sub_df, preserve_index=False)
  iceberg_table.append(pa_sub_table)
  ```
- **효과**: Parquet Row Group Column Statistics (`lower_bounds`, `upper_bounds`)의 min/max 범위가 조밀해져 **BigQuery Row-Group Level Skipping (99.9% 스킵)** 발동.

---

## 📈 4. 5단계 시각화 코드 표준 (Visualization Code Standard)

벤치마킹 결과 시각화 시 **4가지 핵심 다차원 지표**를 4개 서브플롯(Subplots)으로 시각화하고 DPI 300 고해상도 차트로 저장:

```python
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(4, 1, figsize=(14, 24))

# 1. Bytes Processed (MB)
sns.barplot(data=df_bench, x="bytes_processed_mb", y="scenario", hue="table_type", ax=axes[0], palette="viridis")
axes[0].set_title("1. Bytes Processed (Data Scan Size in MB) - Lower is Better", fontsize=14, fontweight="bold")

# 2. Elapsed Time (Seconds)
sns.barplot(data=df_bench, x="elapsed_time_sec", y="scenario", hue="table_type", ax=axes[1], palette="rocket")
axes[1].set_title("2. Elapsed Wall-Clock Time (Execution Time in Seconds) - Lower is Better", fontsize=14, fontweight="bold")

# 3. Slot Millis (ms)
sns.barplot(data=df_bench, x="slot_millis", y="scenario", hue="table_type", ax=axes[2], palette="magma")
axes[2].set_title("3. Slot Millis (CPU Compute Consumption in ms) - Lower is Better", fontsize=14, fontweight="bold")

# 4. Estimated Query Cost ($ USD)
sns.barplot(data=df_bench, x="query_cost_usd", y="scenario", hue="table_type", ax=axes[3], palette="crest")
axes[3].set_title("4. Estimated Query Cost ($ USD @ $6.25/TB On-Demand) - Lower is Better", fontsize=14, fontweight="bold")

plt.tight_layout()
plt.savefig("benchmark_summary_visualization.png", dpi=300, bbox_inches="tight")
```

---

## 🛠️ 5. 주요 트러블슈팅 및 예방 방어 코드 (Defenses)

1. **GCS 버킷/폴더 수동 삭제 시 SQLite 404 NOT_FOUND 캐시 에러 방어**:
   ```python
   if os.path.exists("/tmp/pyiceberg_catalog.db"):
       try:
           os.remove("/tmp/pyiceberg_catalog.db")
       except Exception:
           pass
   ```
2. **PyArrow `__index_level_0__` 컬럼 오류 방어**:
   - `pa.Table.from_pandas(sub_df, preserve_index=False)` 및 `sub_df.reset_index(drop=True)` 지정.
3. **BigQuery Connection 생성**: `google.cloud.bigquery_connection_v1` SDK를 사용하여 Cloud Resource Connection을 파이썬 스크립트 내에서 자동 생성하고 SA Email 추출 후 GCS Storage Admin/Viewer IAM 권한을 자동 바인딩함.
3. **BigQuery External DDL 문법 규격**:
   - 미지원 옵션인 `metadata_cache_mode = 'AUTOMATIC'` 제거 및 `format = 'ICEBERG'` 규격 적용.
4. **Managed vs External Iceberg `Slot Millis` 대 `Elapsed Time` 트레이드오프**:
   - BQ Managed Iceberg: High Parallelism 슬롯 동원으로 Slot Millis(62,080 ms)는 높으나 Elapsed Time(1.18초) 단축.
   - BigLake External Iceberg: 슬롯 동원 제약으로 Slot Millis(35,923 ms)는 적으나 Elapsed Time(1.78초) 소요.
