# 작업 계획

- [x] 차트 미출력 문제와 렌더링 경로 확인
- [x] Plotly 차트 렌더링 오류 수정
- [x] 정적 검증 및 git 점검

# 리뷰

- Plotly 제목 폰트 설정에 지원되지 않는 `weight` 속성이 포함되어 차트 렌더링 단계가 실패하던 문제를 제거함
- `python3 -B -m py_compile app.py`로 문법 검증 완료
- `pytest -q`는 로컬 환경에 `pytest`가 없어 실행하지 못함
