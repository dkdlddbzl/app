let flaskUrl  = 'http://localhost:5000';
let callCount = 0;

// ── 편집 상태 ────────────────────────────────────────────
let currentTable   = '';
let currentColumns = [];
let currentRows    = [];
let pkColumns      = [];
let changes        = new Map();

// ── 초기화 ───────────────────────────────────────────────
async function init() {
  if (window.electronAPI) {
    flaskUrl = await window.electronAPI.getFlaskUrl();
    window.flaskUrl = flaskUrl; // map.js 동기화
    const info = window.electronAPI.appInfo;
    document.getElementById('platform-badge').textContent = info.platform;
  }
  await checkHealth();
}

async function checkHealth() {
  try {
    const res = await fetch(`${flaskUrl}/api/health`);
    setStatus(res.ok);
    addLog(res.ok ? 'ok' : 'err', res.ok ? 'Flask 서버 연결 성공' : '서버 응답 오류');
  } catch {
    setStatus(false);
    addLog('err', 'Flask 서버에 연결할 수 없습니다');
  }
}

function setStatus(ok) {
  document.getElementById('status-dot').className = 'status-dot ' + (ok ? 'ok' : 'err');
  document.getElementById('status-text').textContent = ok ? 'Flask 연결됨' : 'Flask 연결 안됨';
  const s = document.getElementById('stat-status');
  if (s) s.textContent = ok ? 'OK' : 'ERR';
}

function showPage(name, el) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('page-' + name).classList.add('active');
  el.classList.add('active');
  // 맵 페이지 전환 시 초기화
  if (name === 'map' && typeof onMapPageShow === 'function') {
    onMapPageShow();
  }
}

// ── DB 조회 ───────────────────────────────────────────────
async function runQuery() {
  const table   = document.getElementById('inp-table').value.trim();
  const column  = document.getElementById('inp-column').value.trim();
  const value   = document.getElementById('inp-value').value.trim();
  const column2 = document.getElementById('inp-column2').value.trim();
  const value2  = document.getElementById('inp-value2').value.trim();

  if (!table) { showError('테이블명을 입력해주세요.'); return; }
  if ((column2 && !value2) || (!column2 && value2)) {
    showError('AND 컬럼과 검색어를 모두 입력해주세요.'); return;
  }

  const btn = document.getElementById('query-btn');
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner"></div> 조회 중';
  showLoading();
  resetEditState();

  try {
    const res  = await fetch(`${flaskUrl}/api/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table, column, value, column2, value2 }),
    });
    const data = await res.json();
    if (!res.ok) {
      showError(data.error || '조회 실패');
      addLog('err', `조회 실패 [${table}]: ${data.error}`);
      return;
    }

    callCount++;
    document.getElementById('stat-calls').textContent = callCount;

    currentTable   = table;
    currentColumns = data.columns;
    currentRows    = data.rows;
    pkColumns      = data.pk_columns || [];

    let whereStr = '';
    if (column  && value)  whereStr += ` WHERE ${column} = '${value}'`;
    if (column2 && value2) whereStr += ` AND ${column2} = '${value2}'`;
    addLog('ok', `SELECT * FROM ${table}${whereStr} → ${data.count}행`);
    renderTable(data);

  } catch (e) {
    showError('서버 연결 오류: ' + e.message);
    addLog('err', e.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 조회';
  }
}

function resetQuery() {
  ['inp-table','inp-column','inp-value','inp-column2','inp-value2']
    .forEach(id => document.getElementById(id).value = '');
  resetEditState();
  document.getElementById('result-area').innerHTML = `
    <div class="state-box"><div class="state-icon">&#x22A1;</div>
    <span>조회 결과가 여기에 표시됩니다</span></div>`;
  // innerHTML 교체 후 포커스 복원
  setTimeout(() => document.getElementById('inp-table').focus(), 50);
}

// ── 테이블 렌더 ──────────────────────────────────────────
function renderTable({ columns, rows, count, pk_columns }) {
  if (rows.length === 0) {
    document.getElementById('result-area').innerHTML = `
      <div class="state-box"><div class="state-icon">○</div>
      <span>조회 결과가 없습니다</span></div>`;
    return;
  }

  const hasPk     = pkColumns.length > 0;
  const limitNote = count >= 500
    ? `<span style="color:var(--warn)"> (최대 500행 표시)</span>` : '';
  const pkNote = hasPk
    ? `<span style="font-family:var(--mono);font-size:11px;color:var(--text-dim);margin-left:10px">더블클릭으로 셀 편집</span>`
    : `<span style="font-family:var(--mono);font-size:11px;color:var(--warn);margin-left:10px">PK 없음 — 편집 불가</span>`;

  const thead = `<thead><tr>${columns.map(c => {
    const isPk = pkColumns.includes(c);
    return `<th>${c}${isPk ? ' <span style="color:var(--accent);font-size:9px">PK</span>' : ''}</th>`;
  }).join('')}</tr></thead>`;

  const tbody = `<tbody>${rows.map((row, ri) =>
    `<tr data-row="${ri}">${columns.map((c) => {
      const v     = row[c];
      const isPk  = pkColumns.includes(c);
      const edCls = (!isPk && hasPk) ? ' editable' : '';
      const dbl   = (!isPk && hasPk) ? `ondblclick="startEdit(this,${ri})"` : '';
      let cls = edCls, content = '';
      if (v === null || v === undefined) { cls += ' null-val'; content = 'NULL'; }
      else if (typeof v === 'number')    { cls += ' num-val';  content = v; }
      else if (typeof v === 'boolean')   { cls += ' bool-val'; content = String(v); }
      else                               { content = String(v); }
      const safeContent = String(content).replace(/"/g, '&quot;');
      return `<td class="${cls.trim()}" data-col="${c}" data-orig="${safeContent}" title="${safeContent}" ${dbl}>${content}</td>`;
    }).join('')}</tr>`
  ).join('')}</tbody>`;

  document.getElementById('result-area').innerHTML = `
    <div class="result-meta">
      <div class="result-count"><span>${count}</span>행 조회됨${limitNote}${pkNote}</div>
    </div>
    <div class="table-wrap">
      <table class="result">${thead}${tbody}</table>
    </div>`;
}

// ── 셀 편집 ──────────────────────────────────────────────
function startEdit(td, rowIdx) {
  const prev = document.querySelector('td.editing');
  if (prev && prev !== td) finishEdit(prev, false);

  const origVal = td.dataset.orig;
  td.classList.add('editing');
  td.innerHTML = `<input type="text" value="${origVal.replace(/"/g,'&quot;')}" />`;
  const inp = td.querySelector('input');
  inp.focus();
  inp.select();

  inp.addEventListener('blur',    () => finishEdit(td, true));
  inp.addEventListener('keydown', e => {
    if (e.key === 'Enter')  { e.preventDefault(); finishEdit(td, true); }
    if (e.key === 'Escape') { e.preventDefault(); finishEdit(td, false); }
    e.stopPropagation();
  });
}

function finishEdit(td, save) {
  if (!td.classList.contains('editing')) return;
  const inp     = td.querySelector('input');
  const newVal  = inp ? inp.value : td.dataset.orig;
  const origVal = td.dataset.orig;
  const colName = td.dataset.col;
  const rowIdx  = parseInt(td.closest('tr').dataset.row);
  const key     = `${rowIdx}:${colName}`;

  td.classList.remove('editing');

  if (save && newVal !== origVal) {
    const tr = td.closest('tr');
    const pkMap = {};
    pkColumns.forEach(pk => {
      const pkTd = tr.querySelector(`td[data-col="${pk}"]`);
      if (pkTd) pkMap[pk] = pkTd.dataset.orig;
    });
    changes.set(key, { rowIdx, colName, oldVal: origVal, newVal, pkMap });
    td.textContent = newVal;
    td.dataset.orig = newVal;
    td.classList.remove('null-val', 'num-val', 'bool-val');
    td.classList.add('edited');
  } else {
    const orig = td.dataset.orig;
    td.textContent = (orig === 'NULL' || orig === '') ? 'NULL' : orig;
    if (orig === 'NULL') td.classList.add('null-val');
  }
  updateCommitBar();
}

function updateCommitBar() {
  const n = changes.size;
  document.getElementById('change-count').textContent = `${n}개 셀 수정됨`;
  document.getElementById('commit-bar').classList.toggle('visible', n > 0);
}

function resetEditState() {
  changes.clear();
  document.getElementById('commit-bar').classList.remove('visible');
}

// ── 되돌리기 ─────────────────────────────────────────────
function rollbackChanges() {
  changes.forEach(({ rowIdx, colName, oldVal }) => {
    const tr = document.querySelector(`tr[data-row="${rowIdx}"]`);
    if (!tr) return;
    const td = tr.querySelector(`td[data-col="${colName}"]`);
    if (!td) return;
    td.textContent  = oldVal === 'NULL' ? 'NULL' : oldVal;
    td.dataset.orig = oldVal;
    td.classList.remove('edited');
    if (oldVal === 'NULL') td.classList.add('null-val');
  });
  resetEditState();
  addLog('info', '편집 내용을 되돌렸습니다.');
}

// ── 커밋 ─────────────────────────────────────────────────
async function commitChanges() {
  if (changes.size === 0) return;

  const updates = Array.from(changes.values()).map(({ colName, newVal, pkMap }) => ({
    column: colName, value: newVal, pk_map: pkMap,
  }));

  const btn = document.querySelector('.btn-commit');
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner"></div> 저장 중';

  try {
    const res  = await fetch(`${flaskUrl}/api/update`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table: currentTable, updates }),
    });
    const data = await res.json();

    if (!res.ok) {
      addLog('err', `커밋 실패: ${data.error}`);
      showToast(`커밋 실패: ${data.error}`, 'err');
      return;
    }

    addLog('ok', `커밋 완료 — ${data.updated}건 업데이트`);
    document.querySelectorAll('td.edited').forEach(td => td.classList.remove('edited'));
    resetEditState();

  } catch (e) {
    addLog('err', '커밋 오류: ' + e.message);
    showToast('커밋 오류: ' + e.message, 'err');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '✓ 커밋';
  }
}

// ── 서비스 패널 ──────────────────────────────────────────
let selectedShp = '';

async function findServiceFile() {
  const table = document.getElementById('inp-table').value.trim();
  if (!table) { alert('테이블명을 입력해주세요.'); return; }

  // 패널 열기
  const panel = document.getElementById('service-panel');
  panel.classList.add('visible');
  document.getElementById('build-result').classList.remove('visible');
  document.getElementById('dem-input').value = '';
  selectedShp = '';

  // SHP 목록 + 히스토리 불러오기
  const listEl    = document.getElementById('shp-list');
  const histEl    = document.getElementById('shp-history');
  const dirLabel  = document.getElementById('shp-dir-label');
  listEl.innerHTML = '<div class="shp-item" style="cursor:default;color:var(--text-dim)">불러오는 중...</div>';

  try {
    const res  = await fetch(`${flaskUrl}/api/service/shp-list`);
    const data = await res.json();
    dirLabel.textContent = '(' + data.dir + ')';

    // ── 히스토리 렌더 ────────────────────────────────────
    if (data.history && data.history.length > 0) {
      document.getElementById('shp-history-list').innerHTML = data.history.map(f => `
        <div class="shp-item shp-hist-item" style="justify-content:space-between">
          <span onclick="selectShpByName('${f}')" style="flex:1;display:flex;align-items:center;gap:8px">
            <span class="shp-icon" style="color:var(--warn)">★</span>${f}
          </span>
          <button onclick="deleteShpHistory('${f}')" style="background:none;border:none;color:var(--text-dim);cursor:pointer;font-size:13px;padding:0 4px;line-height:1" title="삭제">✕</button>
        </div>`).join('');
    } else {
      document.getElementById('shp-history-list').innerHTML =
        '<div class="shp-item" style="cursor:default;color:var(--text-dim)">최근 선택 기록이 없습니다. 만들기를 실행하면 자동 저장됩니다.</div>';
    }

    // ── 테이블명 기반 자동 선택 (히스토리에 없을 경우 전체 목록에서) ──
    if (!selectedShp && data.files && data.files.length > 0) {
      const base    = table.split('.').pop().toLowerCase();
      const matched = data.files.find(f => f.toLowerCase().includes(base));
      if (matched) {
        selectedShp = matched;
        addLog('info', `SHP 자동 선택: ${matched}`);
      }
    }
  } catch(e) {
    listEl.innerHTML = `<div class="shp-item" style="color:var(--danger)">오류: ${e.message}</div>`;
  }

  // input 포커스
  window.focus();
  setTimeout(() => {
    const inp = document.getElementById('dem-input');
    if (inp) { inp.focus(); inp.click(); }
  }, 50);
}

// 히스토리에서 파일명으로 선택
function selectShpByName(filename) {
  selectedShp = filename;
  // 히스토리 항목 강조
  document.querySelectorAll('.shp-hist-item').forEach(el => {
    el.classList.toggle('selected', el.textContent.trim().startsWith(filename) || el.querySelector('span span') && el.querySelector('span span').nextSibling && el.querySelector('span span').nextSibling.textContent === filename);
  });
  document.querySelectorAll('#shp-list .shp-item').forEach(el => el.classList.remove('selected'));
  addLog('info', `SHP 선택 (히스토리): ${filename}`);
}

// 히스토리 항목 삭제
async function deleteShpHistory(filename) {
  try {
    const res  = await fetch(`${flaskUrl}/api/service/shp-history/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ shp_file: filename }),
    });
    const data = await res.json();
    const histEl   = document.getElementById('shp-history');
    const histList = document.getElementById('shp-history-list');

    if (data.history && data.history.length > 0) {
      histList.innerHTML = data.history.map(f => `
        <div class="shp-item shp-hist-item" style="justify-content:space-between">
          <span onclick="selectShpByName('${f}')" style="flex:1;display:flex;align-items:center;gap:8px">
            <span class="shp-icon" style="color:var(--warn)">★</span>${f}
          </span>
          <button onclick="deleteShpHistory('${f}')" style="background:none;border:none;color:var(--text-dim);cursor:pointer;font-size:13px;padding:0 4px;line-height:1" title="삭제">✕</button>
        </div>`).join('');
    } else {
      histEl.style.display = 'none';
    }
    if (selectedShp === filename) selectedShp = '';
    addLog('info', `히스토리 삭제: ${filename}`);
  } catch(e) {
    addLog('err', '히스토리 삭제 오류: ' + e.message);
  }
}

function selectShp(el, filename) {
  document.querySelectorAll('.shp-item').forEach(i => i.classList.remove('selected'));
  el.classList.add('selected');
  selectedShp = filename;
}

function closeModal() { closeServicePanel(); }

function closeServicePanel() {
  document.getElementById('service-panel').classList.remove('visible');
}

function closeServiceArea() {
  document.getElementById('service-area').style.display = 'none';
}

async function buildService() {
  const table    = document.getElementById('inp-table').value.trim();
  const dem_code = document.getElementById('dem-input').value.trim();

  if (!selectedShp) { alert('SHP 파일을 선택해주세요.'); return; }
  if (!dem_code)     { alert('DEM 코드를 입력해주세요.'); return; }

  const btn = document.getElementById('build-btn');
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner"></div> 실행 중';

  const resultEl = document.getElementById('build-result');
  resultEl.classList.add('visible');

  try {
    const res  = await fetch(`${flaskUrl}/api/service/build`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table, shp_file: selectedShp, dem_code }),
    });
    const data = await res.json();

    if (!res.ok) {
      document.getElementById('br-status').textContent = '실패';
      document.getElementById('br-status').className   = 'br-val err';
      document.getElementById('br-dem').textContent    = data.error || '';
      document.getElementById('br-shp').textContent    = '';
      document.getElementById('br-json').textContent   = '';
      addLog('err', `빌드 실패: ${data.error}`);
      showToast(`빌드 실패: ${data.error}`, 'err');
      return;
    }

    document.getElementById('br-status').textContent = `실행 중 (PID: ${data.pid})`;
    document.getElementById('br-status').className   = 'br-val';
    document.getElementById('br-dem').textContent    = data.dem;
    document.getElementById('br-shp').textContent    = data.shp;
    document.getElementById('br-json').textContent   = data.json_path;
    addLog('ok', `빌더 실행: ${data.message}`);

  } catch(e) {
    document.getElementById('br-status').textContent = '오류: ' + e.message;
    document.getElementById('br-status').className   = 'br-val err';
    addLog('err', '빌드 오류: ' + e.message);
    showToast('빌드 오류: ' + e.message, 'err');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 만들기';
  }
}
// ── SHP 다운로드 ─────────────────────────────────────────
async function exportShp() {
  const table   = document.getElementById('inp-table').value.trim();
  const column  = document.getElementById('inp-column').value.trim();
  const value   = document.getElementById('inp-value').value.trim();
  const column2 = document.getElementById('inp-column2').value.trim();
  const value2  = document.getElementById('inp-value2').value.trim();

  if (!table) { alert('테이블명을 입력해주세요.'); return; }

  const btn = document.getElementById('shp-btn');
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner"></div> 생성 중';

  try {
    const res  = await fetch(`${flaskUrl}/api/export/shp`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ table, column, value, column2, value2 }),
    });
    const data = await res.json();

    if (!res.ok) {
      addLog('err', `SHP 실패: ${data.error}`);
      showToast(`SHP 생성 실패: ${data.error}`, 'err');
      return;
    }

    addLog('ok', `SHP 저장 완료 → ${data.path} (${data.count}건)`);

    // 컬럼명 축약 안내
    if (data.renamed && Object.keys(data.renamed).length > 0) {
      const renames = Object.entries(data.renamed)
        .map(([o, n]) => `  ${o} → ${n}`).join('\n');
      addLog('info', `컬럼명 10자 축약:\n${renames}`);
    }

    showToast(`✓ SHP 저장 완료 (${data.count}건)`, 'ok');

  } catch (e) {
    addLog('err', 'SHP 오류: ' + e.message);
    showToast('SHP 오류: ' + e.message, 'err');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '⬇ SHP 다운로드';
  }
}

// ── 토스트 알림 (alert 대체 — 포커스 빼앗지 않음) ───────────
function showToast(msg, type = 'ok') {
  addLog(type, msg);

  // 인라인 알림 — result-area 상단에 표시 (position:fixed 미사용)
  const colors = {
    ok:   { bg: 'rgba(74,240,168,.1)',  border: 'rgba(74,240,168,.35)',  text: 'var(--accent)'  },
    err:  { bg: 'rgba(240,90,90,.1)',   border: 'rgba(240,90,90,.35)',   text: 'var(--danger)'  },
    info: { bg: 'rgba(61,212,240,.1)',  border: 'rgba(61,212,240,.35)',  text: 'var(--accent2)' },
    warn: { bg: 'rgba(240,168,74,.1)',  border: 'rgba(240,168,74,.35)',  text: 'var(--warn)'    },
  };
  const c = colors[type] || colors.ok;

  const existing = document.getElementById('inline-toast');
  if (existing) existing.remove();

  const el = document.createElement('div');
  el.id = 'inline-toast';
  el.style.cssText = `
    padding: 10px 14px;
    background: ${c.bg};
    border: 1px solid ${c.border};
    border-radius: var(--radius);
    color: ${c.text};
    font-family: var(--mono);
    font-size: 12px;
    margin-bottom: 8px;
    animation: fadeUp .2s ease;
    pointer-events: none;
    -webkit-app-region: no-drag;
  `;
  el.textContent = msg;

  const area = document.getElementById('result-area');
  if (area) area.parentNode.insertBefore(el, area);

  setTimeout(() => el.remove(), 3500);
}

// ── 상태 화면 ────────────────────────────────────────────
function showLoading() {
  document.getElementById('result-area').innerHTML =
    `<div class="state-box"><div class="spinner"></div><span>조회 중...</span></div>`;
}

function showError(msg) {
  document.getElementById('result-area').innerHTML =
    `<div class="state-box"><div class="state-icon" style="color:var(--danger)">✕</div>
    <span style="color:var(--danger)">${msg}</span></div>`;
}

// ── 로그 ─────────────────────────────────────────────────
function addLog(type, msg) {
  const box  = document.getElementById('log-box');
  const time = new Date().toTimeString().slice(0, 8);
  const line = document.createElement('div');
  line.className = 'log-line';
  line.innerHTML = `<span class="log-time">${time}</span><span class="log-${type}">${msg}</span>`;
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}

function clearLog() { document.getElementById('log-box').innerHTML = ''; }

// ── 전역 함수 노출 (외부 HTML onclick에서 접근 가능하도록) ──
window.flaskUrl         = flaskUrl;  // map.js에서 사용
window.showPage         = showPage;
window.runQuery         = runQuery;
window.resetQuery       = resetQuery;
window.exportShp        = exportShp;
window.findServiceFile  = findServiceFile;
window.selectShp        = selectShp;
window.selectShpByName  = selectShpByName;
window.deleteShpHistory = deleteShpHistory;
window.closeServicePanel = closeServicePanel;
window.closeServiceArea = closeServiceArea;
window.closeModal       = closeModal;
window.buildService     = buildService;
window.rollbackChanges  = rollbackChanges;
window.commitChanges    = commitChanges;
window.startEdit        = startEdit;
window.clearLog         = clearLog;
window.addLog           = addLog;

init();