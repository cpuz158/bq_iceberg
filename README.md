# BigQuery & Apache Iceberg 50GB Enterprise Benchmark Suite 🚀

[![BigQuery](https://img.shields.io/badge/Google_Cloud-BigQuery-4285F4?style=flat-square&logo=googlecloud)](https://cloud.google.com/bigquery)
[![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-blue?style=flat-square&logo=apache)](https://iceberg.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.2.0-E25A1C?style=flat-square&logo=apachespark)](https://spark.apache.org/)
[![PyIceberg](https://img.shields.io/badge/PyIceberg-0.11.0-8CA1AF?style=flat-square)](https://pyiceberg.databend.rs/)

본 저장소는 **BigQuery Native (Capacitor)**와 **Apache Iceberg (Managed, External, Metastore)** 간의 **50GB (1.5억 건) 대용량 웹로그 데이터셋**을 기반으로 한 엔터프라이즈 성능 벤치마크 및 심층 파일 레이아웃 분석 자동화 프로젝트입니다.

---

## 📌 주요 산출물 명세 (Deliverables)

| 산출물 파일명 | 유형 | 핵심 기능 및 상세 설명 |
| :--- | :--- | :--- |
| **[`GEMINI.md`](./GEMINI.md)** | **Architecture Guide** | 프로젝트 설계 표준, 에이전트 미션, 4대 테이블 규격 및 코드 레벨 재구현 가이드라인 |
| **[`bq_iceberg_deepdive.ipynb`](./bq_iceberg_deepdive.ipynb)** | **Main Notebook** | 1.5억 건 데이터 적재, 파티셔닝/클러스터 정렬, 벤치마킹 쿼리 및 시각화 완전 자동화 주피터 노트북 |
| **[`bq_iceberg_analysis.ipynb`](./bq_iceberg_analysis.ipynb)** | **Analysis Notebook** | Partition Pruning, Predicate Pushdown, File/Row Group Layout 심층 실증 전용 노트북 |
| **[`bq_iceberg_deepdive_report.md`](./bq_iceberg_deepdive_report.md)** | **Technical Report** | 11개 핵심 용어 정의, 아키텍처 비교표, Slot Millis 대 Elapsed Time 트레이드오프 종합 보고서 |
| **[`benchmark_summary_visualization.png`](./benchmark_summary_visualization.png)** | **Visualization** | Bytes Processed, Elapsed Time, Slot Millis, Estimated Cost 4종 다차원 고해상도 시각화 차트 |
| **[`pyproject.toml`](./pyproject.toml)** / **[`requirements.txt`](./requirements.txt)** | **Dependencies** | PySpark 4.2.0, PyIceberg 0.11.0+, BigQuery SDK 패키지 호환성 정의서 |

---

## 📊 4대 대조군 테이블 비교

본 프로젝트에서는 150,000,000건 (1.5억 건, ~50GB)의 동일한 시드 데이터로 아래 4개 아키텍처 테이블을 구축하여 성능을 측정합니다:

1. **`native_weblog`**: BigQuery Native Table (`PARTITION BY event_date CLUSTER BY user_id, event_type`)
2. **`managed_iceberg_weblog`**: BigQuery Managed Iceberg Table (BQ 스토리지/메타데이터 자동 관리)
3. **`external_iceberg_weblog`**: LakeHouse External Iceberg Table (`PyIceberg` & GCS Open Storage)
4. **`metastore_iceberg_weblog`**: BigQuery Metastore Iceberg Table (`PySpark` / Metastore API)

---

## ⚡ 핵심 엔지니어링 하이라이트

- **PyIceberg Fast Bulk Append Pipeline**:
  - DML 제약 회피 및 GCS 커밋 오버헤드 최소화를 위해 날짜별 1,500만 건 대용량 배치 커밋 적용 (수십 초 내 1.5억 건 적재 완성).
- **BigQuery `CLUSTER BY` 와 100% 동일한 Predicate Pushdown 구현**:
  - `write.sort.order` 프로퍼티 세팅 및 PyArrow `preserve_index=False` 물리적 정렬을 결합하여 Parquet Row Group Statistics min/max 범위를 축소 → **Row-Group Level Skipping (99.9% 스킵)** 구동.
- **다차원 성능 시각화 (4종 지표)**:
  - 데이터 스캔 용량(MB), 체감 실행 시간(초), 슬롯 소모량(ms), 예측 실행 비용($ USD) 4개 서브플롯 통합 시각화 제공.

---

## 🚀 빠른 시작 (Quick Start)

```bash
# 1. 저장소 클론
git clone git@github.com:cpuz158/bq_iceberg.git
cd bq_iceberg

# 2. 파이썬 가상환경 구성 및 패키지 설치
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. 주피터 노트북 실행 후 [프로젝트 설정] 셀에서 본인의 GCP Project ID 설정
# USER_PROJECT_ID = "your-gcp-project-id" (미입력 시 gcloud 로그인 ADC 환경 자동 감지)
jupyter lab bq_iceberg_deepdive.ipynb
```

---

## 📝 라이선스 & 환경
- **GCP Project**: `your-gcp-project-id`
- **Region**: `asia-northeast3` (서울 리전)
- **Engine**: PySpark 4.2.0, PyIceberg 0.11.0+, JDK 17+/26+ Compatible
