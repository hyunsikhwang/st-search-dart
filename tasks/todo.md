# 작업 계획

- [x] `automation.py`의 `processing_status.last_base_period` 사용 흐름과 재조회 누락 원인을 확인
- [x] 현재 조회 기준월보다 과거에 처리된 회사가 재처리 대상에 포함되도록 자동화 쿼리 수정
- [x] 정적 검증, git 점검, 커밋/푸시 수행

# 리뷰

- `processing_status.last_base_period`가 현재 조회 기준월보다 작은 경우 재조회 대상으로 다시 포함되도록 `automation.py` 쿼리를 수정함
- `last_base_period`가 비어있거나 숫자 변환이 안 되는 상태도 재처리 대상으로 포함해 상태 이상치에 막히지 않도록 보완함
- `python3 -B -m py_compile automation.py`로 문법 검증 완료
- `pytest -q`는 로컬 환경에 `pytest` 명령이 없어 실행하지 못함
