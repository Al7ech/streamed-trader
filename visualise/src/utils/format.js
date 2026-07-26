// 공용 표시 포맷 헬퍼 (InfoPanel / RunListPage 등에서 재사용)

export const fmtDate = (iso) => {
  if (!iso) return '-';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().slice(0, 10);
};

export const fmtDateTime = (iso) => {
  if (!iso) return '-';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().slice(0, 16).replace('T', ' ');
};

export const fmtPct = (v) => (v === undefined || v === null ? '-' : `${(v * 100).toFixed(2)}%`);

// 부호를 명시하는 % 표기 (+12.3% / -4.1%) — KPI 타일·월별 바 라벨 등 수익률 표시용
export const fmtSignedPct = (v, digits = 1) =>
  (v === undefined || v === null ? '-' : `${v > 0 ? '+' : ''}${(v * 100).toFixed(digits)}%`);

export const fmtNum = (v, digits = 2) => (v === undefined || v === null ? '-' : Number(v).toFixed(digits));

// 초 단위 기간을 사람이 읽는 형태로: 2d 미만은 시간, 그 이상은 일 단위 (예: "18.5h", "3.2d")
export const fmtDuration = (sec) => {
  if (sec === undefined || sec === null) return '-';
  const hours = sec / 3600;
  if (hours < 48) return `${hours.toFixed(1)}h`;
  return `${(hours / 24).toFixed(1)}d`;
};

export const paramsString = (params) => {
  if (!params || typeof params !== 'object') return '';
  return Object.entries(params).map(([k, v]) => `${k}=${v}`).join(', ');
};

// 목록용 초압축 params 표기: 값만 나열 (예: "4500, 360, 180, 0.0004, 0.08")
export const paramsValues = (params) => {
  if (!params || typeof params !== 'object') return '';
  return Object.values(params).join(', ');
};

// tooltip용: 한 줄에 하나씩 "key: value" (whiteSpace: pre-line과 함께 사용)
export const paramsLines = (params) => {
  if (!params || typeof params !== 'object') return '';
  return Object.entries(params).map(([k, v]) => `${k}: ${v}`).join('\n');
};
