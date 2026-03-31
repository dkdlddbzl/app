// ── 전역 상태 ─────────────────────────────────────────────
let mapMeta              = null;   // 시설물/시도 메타
let lastExtractedShpPath = null;   // 마지막 추출 SHP 전체 경로
let cmpAllData           = [];     // 전체 비교 결과
let cmpMisData           = [];     // 미일치 결과
let cmpDepCol            = 'man_dep';

// ── 메타 로드 ────────────────────────────────────────────
async function loadMapMeta() {
  try {
    const res = await fetch(`${window.flaskUrl}/api/map/meta`);
    mapMeta   = await res.json();
    renderPtDivSelect();
    renderPtSidoSelect();
    renderCmpDivSelect();
    renderCmpSidoSelect();
  } catch(e) {
    addLog('err', '맵 메타 로드 실패: ' + e.message);
  }
}

// ── 포인트 추출 — 구분/시도 셀렉트 렌더 ─────────────────
function renderPtDivSelect() {
  if (!mapMeta) return;
  const sel = document.getElementById('pt-div');
  sel.innerHTML = '<option value="">선택</option>' +
    Object.entries(mapMeta.facilities).map(([k, v]) =>
      `<option value="${k}">${v.label} (${k})</option>`
    ).join('');
}

function renderPtSidoSelect() {
  if (!mapMeta) return;
  const sel = document.getElementById('pt-sido');
  sel.innerHTML = '<option value="">선택</option>' +
    mapMeta.sido.map(s => `<option value="${s.code}">${s.label} (${s.code})</option>`).join('');
}

function onPtDivChange() {
  const div    = document.getElementById('pt-div').value;
  const facSel = document.getElementById('pt-facility');
  facSel.innerHTML = '<option value="">선택</option>';

  if (div && mapMeta?.facilities[div]) {
    // 3D 관로만 필터링
    Object.entries(mapMeta.facilities[div].facilities)
      .filter(([k]) => k.endsWith('_3D'))
      .forEach(([k, v]) => {
        facSel.innerHTML += `<option value="${k}">${v.label}</option>`;
      });
  }
  updatePtTableLabel();
}

function updatePtTableLabel() {
  const div      = document.getElementById('pt-div').value;
  const facility = document.getElementById('pt-facility').value;
  const sido     = document.getElementById('pt-sido').value;
  const label    = document.getElementById('pt-table-label');
  if (div && facility && sido) {
    label.textContent = `${div}_${facility}_${sido}000`;
  } else {
    label.textContent = '—';
  }
}

// ── 포인트 추출 (SSE) ────────────────────────────────────
async function extractPoints() {
  const div        = document.getElementById('pt-div').value;
  const facility   = document.getElementById('pt-facility').value;
  const sido       = document.getElementById('pt-sido').value;
  const sigunguRaw = document.getElementById('pt-sigungu').value.trim();
  const sigungu    = sigunguRaw
    ? (sigunguRaw.endsWith('%') ? sigunguRaw : sigunguRaw + '%')
    : '';

  if (!div || !facility || !sido) {
    alert('구분, 시설물(3D), 시도를 선택해주세요.');
    return;
  }

  const btn = document.getElementById('pt-extract-btn');
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner"></div> 추출 중';

  addLog('info', `포인트 추출 시작: ${div}_${facility}_${sido}000`);

  try {
    const res = await fetch(`${window.flaskUrl}/api/map/extract-points`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ div, facility, sido, sigungu }),
    });

    const reader  = res.body.getReader();
    const decoder = new TextDecoder();
    let   buffer  = '';
    let   done_data = null;

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const parsed = JSON.parse(line.slice(6));
          const type   = parsed.type;
          const msg    = parsed.msg;

          if (type === 'done') {
            done_data = JSON.parse(msg);
          } else if (type === 'error') {
            addLog('err', msg);
          } else if (type === 'warn') {
            addLog('warn', msg);
          } else {
            addLog('info', msg);
          }
        } catch(e) {}
      }
    }

    if (done_data) {
      // 추출된 SHP 경로 저장
      lastExtractedShpPath = done_data.path;
      const shpLabel = document.getElementById('cmp-shp-label');
      if (shpLabel) shpLabel.textContent = done_data.shp_name;

      showToast(`◎ 추출 완료: ${done_data.count}건 → ${done_data.shp_name}`, 'ok');
      addLog('ok', `저장 완료: ${done_data.path} (${done_data.count}건)`);
    }

  } catch(e) {
    showToast('추출 오류: ' + e.message, 'err');
    addLog('err', '추출 오류: ' + e.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '◎ 추출';
  }
}

// ── 심도 비교 — 구분/시설물/시도 셀렉트 ─────────────────
function renderCmpDivSelect() {
  if (!mapMeta) return;
  const sel = document.getElementById('cmp-div');
  sel.innerHTML = '<option value="">선택</option>' +
    Object.entries(mapMeta.facilities).map(([k, v]) =>
      `<option value="${k}">${v.label} (${k})</option>`
    ).join('');
}

function renderCmpSidoSelect() {
  if (!mapMeta) return;
  const sel = document.getElementById('cmp-sido');
  sel.innerHTML = '<option value="">선택</option>' +
    mapMeta.sido.map(s => `<option value="${s.code}">${s.label} (${s.code})</option>`).join('');
}

function onCmpDivChange() {
  const div    = document.getElementById('cmp-div').value;
  const facSel = document.getElementById('cmp-facility');
  facSel.innerHTML = '<option value="">선택</option>';
  if (div && mapMeta?.facilities[div]) {
    Object.entries(mapMeta.facilities[div].facilities).forEach(([k, v]) => {
      facSel.innerHTML += `<option value="${k}">${v.label} (${k})</option>`;
    });
  }
  updateCmpTableLabel();
}

function updateCmpTableLabel() {
  const div      = document.getElementById('cmp-div').value;
  const facility = document.getElementById('cmp-facility').value;
  const sido     = document.getElementById('cmp-sido').value;
  const label    = document.getElementById('cmp-table-label');
  if (div && facility && sido) {
    label.textContent = `${div}_${facility}_${sido}000`;
  } else {
    label.textContent = '—';
  }
}

// ── 심도 비교 ─────────────────────────────────────────────
async function runDepthComparison() {
  const facilityType = document.getElementById('cmp-type').value;
  const div          = document.getElementById('cmp-div').value;
  const facility     = document.getElementById('cmp-facility').value;
  const sido         = document.getElementById('cmp-sido').value;
  const sigunguRaw   = document.getElementById('cmp-sigungu').value.trim();
  const sigungu      = sigunguRaw
    ? (sigunguRaw.endsWith('%') ? sigunguRaw : sigunguRaw + '%')
    : '';

  if (!div || !facility || !sido) {
    alert('구분, 시설물, 시도를 선택해주세요.');
    return;
  }

  const table = `${div}_${facility}_${sido}000`;

  if (!lastExtractedShpPath) {
    alert('먼저 포인트 추출을 실행해주세요.');
    return;
  }

  const btn = document.getElementById('cmp-run-btn');
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner"></div>';

  addLog('info', `심도 비교 시작: ${table}${sigungu ? ' (' + sigungu + ')' : ''} [${facilityType === 'manhol' ? '맨홀/man_dep' : '밸브/val_dep'}]`);

  try {
    const res = await fetch(`${window.flaskUrl}/api/map/compare-depth`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        shp_path:      lastExtractedShpPath,
        facility_type: facilityType,
        table,
        sigungu,
      }),
    });
    const data = await res.json();

    if (!res.ok) {
      addLog('err', `비교 오류: ${data.error}`);
      showToast(`오류: ${data.error}`, 'err');
      return;
    }

    cmpAllData = data.all || [];
    cmpMisData = data.mismatches || [];
    cmpDepCol  = data.dep_col || (facilityType === 'manhol' ? 'man_dep' : 'val_dep');

    addLog('ok', `심도 비교 완료: 전체 ${data.total}건 | 일치 ${data.match_count}건 | 미일치 ${data.mismatch_count}건`);

    // 탭 카운트 업데이트
    document.getElementById('tab-all-count').textContent = `(${data.total})`;
    document.getElementById('tab-mis-count').textContent = `(${data.mismatch_count})`;

    // 결과 영역 표시
    document.getElementById('cmp-empty').style.display = 'none';
    const area = document.getElementById('cmp-table-area');
    area.style.display = 'flex';

    // 전체 탭으로 기본 표시
    switchCmpTab('all');

  } catch(e) {
    addLog('err', '비교 오류: ' + e.message);
    showToast('비교 오류: ' + e.message, 'err');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 비교';
  }
}

function switchCmpTab(tab) {
  const allBtn = document.getElementById('tab-all');
  const misBtn = document.getElementById('tab-mis');

  if (tab === 'all') {
    allBtn.className = 'btn btn-primary';
    misBtn.className = 'btn btn-ghost';
    renderCmpTable(cmpAllData);
  } else {
    allBtn.className = 'btn btn-ghost';
    misBtn.className = 'btn btn-primary';
    renderCmpTable(cmpMisData);
  }
}

function renderCmpTable(rows) {
  const tbl = document.getElementById('cmp-table');

  if (!rows || !rows.length) {
    tbl.innerHTML = `
      <tr><td colspan="7"
        style="text-align:center;color:var(--text-dim);padding:30px;font-family:var(--mono);font-size:12px">
        데이터 없음
      </td></tr>`;
    return;
  }

  const thStyle = 'padding:8px 12px;text-align:left;font-family:var(--mono);font-size:11px;color:var(--text-dim);letter-spacing:.04em;text-transform:uppercase;border-bottom:1px solid var(--border);white-space:nowrap;background:var(--bg2)';
  const tdStyle = 'padding:7px 12px;font-family:var(--mono);font-size:12px;border-bottom:1px solid rgba(37,42,50,.4);white-space:nowrap';

  const header = `
    <thead>
      <tr>
        <th style="${thStyle}">No</th>
        <th style="${thStyle}">X</th>
        <th style="${thStyle}">Y</th>
        <th style="${thStyle}">${cmpDepCol} (DB)</th>
        <th style="${thStyle}">추출심도</th>
        <th style="${thStyle}">차이</th>
        <th style="${thStyle}">일치</th>
      </tr>
    </thead>`;

  const bodyRows = rows.map(r => {
    const matchColor = r.match ? 'var(--accent)' : 'var(--danger)';
    const matchIcon  = r.match ? '✓' : '✗';
    const rowBg      = r.match ? '' : 'background:rgba(255,80,80,.04)';
    return `
      <tr style="${rowBg}">
        <td style="${tdStyle};color:var(--text-dim)">${r.no}</td>
        <td style="${tdStyle}">${r.x ?? '—'}</td>
        <td style="${tdStyle}">${r.y ?? '—'}</td>
        <td style="${tdStyle}">${r[cmpDepCol] ?? '—'}</td>
        <td style="${tdStyle}">${r.dep_ext ?? '—'}</td>
        <td style="${tdStyle};color:${r.diff !== null && r.diff !== undefined ? (r.match ? 'var(--text-dim)' : 'var(--warn)') : 'var(--text-dim)'}">${r.diff ?? '—'}</td>
        <td style="${tdStyle};color:${matchColor};font-weight:500">${matchIcon}</td>
      </tr>`;
  }).join('');

  tbl.innerHTML = header + '<tbody>' + bodyRows + '</tbody>';
}

// ── 페이지 진입 시 ───────────────────────────────────────
function onMapPageShow() {
  if (!mapMeta) {
    loadMapMeta();
  }
}

// ── window 노출 ──────────────────────────────────────────
window.onMapPageShow       = onMapPageShow;
window.onPtDivChange       = onPtDivChange;
window.updatePtTableLabel  = updatePtTableLabel;
window.extractPoints       = extractPoints;
window.onCmpDivChange      = onCmpDivChange;
window.updateCmpTableLabel = updateCmpTableLabel;
window.runDepthComparison  = runDepthComparison;
window.switchCmpTab        = switchCmpTab;
