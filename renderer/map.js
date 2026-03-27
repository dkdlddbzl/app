// ── 맵 전역 상태 ──────────────────────────────────────────
let olMap        = null;
let vectorSource = null;
let vectorLayer  = null;
let mapMeta      = null;   // 시설물/시도 메타
let currentEntry = null;   // 현재 선택된 조건

// ── 맵 초기화 ────────────────────────────────────────────
function initMap() {
  if (olMap) return;
  if (typeof ol === 'undefined') {
    document.getElementById('map-container').innerHTML =
      '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--danger);font-family:var(--mono);font-size:12px;text-align:center;padding:20px">ol.js 파일이 없습니다.<br>renderer/ol.js 를 추가해주세요.</div>';
    return;
  }

  vectorSource = new ol.source.Vector();
  vectorLayer  = new ol.layer.Vector({ source: vectorSource, style: mapStyleFn });

  olMap = new ol.Map({
    target: 'map-container',
    layers: [
      // 오프라인 환경 — 단색 배경 (OSM 타일 없음)
      vectorLayer,
    ],
    view: new ol.View({
      center: ol.proj.fromLonLat([127.5, 36.5]),
      zoom: 7,
    }),
    controls: ol.control.defaults.defaults({ attribution: false }),
  });

  olMap.on('pointermove', e => {
    const c = ol.proj.toLonLat(e.coordinate);
    const el = document.getElementById('map-coord');
    if (el) el.textContent = `${c[1].toFixed(5)}, ${c[0].toFixed(5)}`;
    olMap.getTargetElement().style.cursor = olMap.hasFeatureAtPixel(e.pixel) ? 'pointer' : '';
  });

  olMap.on('click', e => {
    const feats = [];
    olMap.forEachFeatureAtPixel(e.pixel, f => feats.push(f));
    if (feats.length) showMapPopup(feats[0]);
    else closeMapPopup();
  });
}

// ── 스타일 ───────────────────────────────────────────────
const COLORS = {
  // 상수
  'WTL_VALV_PS':    '#4af0a8',
  'WTL_PIPE_LM_2D': '#3dd4f0',
  'WTL_PIPE_LM_3D': '#1fa8c0',
  'WTL_MANH_PS':    '#1db87a',
  // 하수
  'SWL_PIPE_LM_2D': '#f0a84a',
  'SWL_PIPE_LM_3D': '#c07820',
  'SWL_MANH_PS':    '#c47a20',
  // 시설물통합 — 가스 (G)
  'UFL_GPIP_LM_2D': '#f05a5a',
  'UFL_GPIP_LM_3D': '#c03030',
  'UFL_GVAL_PS':    '#e02020',
  'UFL_GMAN_PS':    '#902020',
  // 시설물통합 — 전력 (B)
  'UFL_BPIP_LM_2D': '#f0d84a',
  'UFL_BPIP_LM_3D': '#c0a820',
  'UFL_BMAN_PS':    '#b09820',
  // 시설물통합 — 통신 (K)
  'UFL_KPIP_LS_2D': '#a89fec',
  'UFL_KPIP_LS_3D': '#8070c0',
  'UFL_KMAN_PS':    '#7060c0',
  // 시설물통합 — 난방 (H)
  'UFL_HPIP_LM_2D': '#f08040',
  'UFL_HPIP_LM_3D': '#c05010',
  'UFL_HMAN_PS':    '#b05010',
};

function mapStyleFn(feature) {
  const key   = feature.get('_layer_key') || '';
  const color = COLORS[key] || '#4af0a8';
  const type  = feature.getGeometry().getType();

  if (type === 'Point' || type === 'MultiPoint') {
    return new ol.style.Style({
      image: new ol.style.Circle({
        radius: 5,
        fill:   new ol.style.Fill({ color }),
        stroke: new ol.style.Stroke({ color: '#0d0f11', width: 1.5 }),
      }),
    });
  }
  if (type.includes('Line')) {
    return new ol.style.Style({
      stroke: new ol.style.Stroke({ color, width: 2.5 }),
    });
  }
  return new ol.style.Style({
    fill:   new ol.style.Fill({ color: color + '33' }),
    stroke: new ol.style.Stroke({ color, width: 2 }),
  });
}

// ── 메타 로드 ────────────────────────────────────────────
async function loadMapMeta() {
  try {
    const res  = await fetch(`${window.flaskUrl}/api/map/meta`);
    mapMeta    = await res.json();
    // 조회 섹션
    renderDivSelect();
    renderSidoSelect();
    // 포인트 추출 섹션
    renderPtDivSelect();
    renderPtSidoSelect();
    loadIndexMapList();
  } catch(e) {
    addLog('err', '맵 메타 로드 실패: ' + e.message);
  }
}

// ── 구분 셀렉트 ──────────────────────────────────────────
function renderDivSelect() {
  const sel = document.getElementById('map-div');
  sel.innerHTML = '<option value="">선택</option>' +
    Object.entries(mapMeta.facilities).map(([k, v]) =>
      `<option value="${k}">${v.label} (${k})</option>`
    ).join('');
}

function onMapDivChange() {
  const div = document.getElementById('map-div').value;
  const facSel = document.getElementById('map-facility');
  facSel.innerHTML = '<option value="">선택</option>';
  if (div && mapMeta?.facilities[div]) {
    Object.entries(mapMeta.facilities[div].facilities).forEach(([k, v]) => {
      facSel.innerHTML += `<option value="${k}">${v.label} (${k})</option>`;
    });
  }
  updateTableLabel();
  resetSigungu();
}

function onMapFacilityChange() {
  updateTableLabel();
  resetSigungu();
}

// ── 시도 셀렉트 ──────────────────────────────────────────
function renderSidoSelect() {
  const sel = document.getElementById('map-sido');
  sel.innerHTML = '<option value="">선택</option>' +
    mapMeta.sido.map(s => `<option value="${s.code}">${s.label} (${s.code})</option>`).join('');
}

function onMapSidoChange() {
  updateTableLabel();
}

function resetSigungu() {
  document.getElementById('map-sigungu').value = '';
}

function updateTableLabel() {
  const div      = document.getElementById('map-div').value;
  const facility = document.getElementById('map-facility').value;
  const sido     = document.getElementById('map-sido').value;
  const label    = document.getElementById('map-table-label');
  if (div && facility && sido) {
    label.textContent = `${div}_${facility}_${sido}000`;
  } else {
    label.textContent = '—';
  }
}

// ── 맵 데이터 로드 ───────────────────────────────────────
async function loadMapData() {
  if (!olMap) initMap();
  if (!vectorSource) { alert('지도 초기화 실패. 맵 탭을 다시 클릭해주세요.'); return; }

  const div      = document.getElementById('map-div').value;
  const facility = document.getElementById('map-facility').value;
  const sido     = document.getElementById('map-sido').value;
  const sigunguRaw = document.getElementById('map-sigungu').value.trim();
  // 숫자만 입력했으면 % 자동 추가, 이미 %있으면 그대로
  const sigungu = sigunguRaw
    ? (sigunguRaw.endsWith('%') ? sigunguRaw : sigunguRaw + '%')
    : '';

  if (!div || !facility || !sido) { alert('구분, 시설물, 시도를 선택해주세요.'); return; }

  const table    = `${div}_${facility}_${sido}000`;
  const layerKey = `${div}_${facility}`;

  currentEntry = { div, facility, sido, sigungu, table, layerKey };

  const btn = document.getElementById('map-load-btn');
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner"></div>';

  try {
    const res  = await fetch(`${window.flaskUrl}/api/map/geojson`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ div, facility, sido, sigungu }),
    });
    const data = await res.json();

    if (!res.ok) {
      showToast(`맵 로드 실패: ${data.error}`, 'err');
      addLog('err', `맵 로드 실패: ${data.error}`);
      return;
    }

    vectorSource.clear();
    const format   = new ol.format.GeoJSON();
    const features = format.readFeatures(data, { featureProjection: 'EPSG:3857' });
    features.forEach(f => f.set('_layer_key', layerKey));
    vectorSource.addFeatures(features);

    document.getElementById('map-table-label').textContent = table;
    document.getElementById('map-count-label').textContent = `${data.count}건`;
    renderLayerFilter(table, layerKey, data.geom_type);
    mapFitExtent();
    addLog('ok', `맵 조회: ${table}${sigungu ? ' WHERE hjd_cde LIKE \'' + sigungu + '\'' : ''} → ${data.count}건`);

  } catch(e) {
    showToast('맵 오류: ' + e.message, 'err');
    addLog('err', '맵 오류: ' + e.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '▶ 조회';
  }
}

// ── 레이어 필터 ──────────────────────────────────────────
function renderLayerFilter(table, layerKey, geomType) {
  const color = COLORS[layerKey] || '#4af0a8';
  const icon  = geomType === 'Point' ? '●' : geomType === 'Line' ? '━' : '■';
  document.getElementById('map-layer-filter').innerHTML = `
    <label style="display:flex;align-items:center;gap:5px;cursor:pointer;font-family:var(--mono);font-size:11px;color:var(--text-dim);background:rgba(13,15,17,.8);padding:3px 8px;border-radius:4px">
      <input type="checkbox" checked onchange="toggleLayer(this.checked)"
        style="-webkit-app-region:no-drag;width:12px;height:12px;accent-color:${color};cursor:pointer" />
      <span style="color:${color}">${icon}</span>${table.split('.').pop()}
    </label>`;
}

function toggleLayer(visible) {
  if (vectorLayer) vectorLayer.setVisible(visible);
}

function mapFitExtent() {
  if (!olMap || !vectorSource || !vectorSource.getFeatures().length) return;
  olMap.getView().fit(vectorSource.getExtent(), { padding: [40,40,40,40], duration: 400, maxZoom: 18 });
}

// ── 속성 팝업 ────────────────────────────────────────────
function showMapPopup(feature) {
  const props   = feature.getProperties();
  const content = document.getElementById('map-popup-content');
  const rows = Object.entries(props)
    .filter(([k]) => !k.startsWith('_') && k !== feature.getGeometryName())
    .map(([k, v]) => `
      <div style="display:flex;gap:8px;padding:3px 0;border-bottom:1px solid rgba(37,42,50,.5)">
        <span style="color:var(--text-dim);min-width:80px;font-size:10px;flex-shrink:0">${k}</span>
        <span style="color:var(--text);word-break:break-all;font-size:11px">${v ?? 'NULL'}</span>
      </div>`).join('');
  content.innerHTML = rows || '<span style="color:var(--text-dim)">속성 없음</span>';
  document.getElementById('map-popup').style.display = 'block';
}

function closeMapPopup() {
  document.getElementById('map-popup').style.display = 'none';
}

// ── 인덱스맵 저장 ─────────────────────────────────────────
function openSaveIndexMap() {
  if (!currentEntry) { alert('먼저 조회를 실행해주세요.'); return; }
  const panel = document.getElementById('save-index-panel');
  panel.style.display = 'block';
  const inp = document.getElementById('index-map-name');
  // 기본 이름 자동 생성
  const divMeta = mapMeta?.facilities[currentEntry.div];
  const facMeta = divMeta?.facilities[currentEntry.facility];
  const sidoLabel = mapMeta?.sido.find(s => s.code === currentEntry.sido)?.label || currentEntry.sido;
  inp.value = `${divMeta?.label || ''} ${facMeta?.label || ''} ${sidoLabel}${currentEntry.sigungu ? ' ' + currentEntry.sigungu : ''}`.trim();
  setTimeout(() => inp.focus(), 50);
}

async function saveIndexMap() {
  const name = document.getElementById('index-map-name').value.trim();
  if (!name) { alert('이름을 입력해주세요.'); return; }
  if (!currentEntry) { alert('조회 조건이 없습니다.'); return; }

  try {
    const res  = await fetch(`${window.flaskUrl}/api/map/index/save`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, entry: currentEntry }),
    });
    const data = await res.json();
    document.getElementById('save-index-panel').style.display = 'none';
    renderIndexMapList(data.index_map);
    addLog('ok', `인덱스맵 저장: ${name}`);
    showToast(`★ 저장됨: ${name}`, 'ok');
  } catch(e) {
    addLog('err', '인덱스맵 저장 오류: ' + e.message);
  }
}

async function loadIndexMapList() {
  try {
    const res  = await fetch(`${window.flaskUrl}/api/map/index/list`);
    const data = await res.json();
    renderIndexMapList(data.index_map);
  } catch(e) {}
}

function renderIndexMapList(indexMap) {
  const list = document.getElementById('index-map-list');
  const entries = Object.entries(indexMap || {});
  if (entries.length === 0) {
    list.innerHTML = '<div style="font-family:var(--mono);font-size:11px;color:var(--text-dim);padding:8px 4px">저장된 항목이 없습니다</div>';
    return;
  }
  list.innerHTML = entries.map(([name, entry]) => {
    const color = COLORS[entry.layerKey] || '#4af0a8';
    return `
      <div style="background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:8px 10px;cursor:pointer;transition:background .1s"
        onmouseover="this.style.background='var(--bg3)'"
        onmouseout="this.style.background='var(--bg2)'"
        onclick="applyIndexMap(${JSON.stringify(JSON.stringify(entry))})">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <span style="font-family:var(--mono);font-size:11px;color:${color};font-weight:500">${name}</span>
          <button onclick="event.stopPropagation();deleteIndexMap('${name.replace(/'/g,"\\\'")}')"
            style="background:none;border:none;color:var(--text-dim);cursor:pointer;font-size:12px;padding:0 2px;line-height:1">✕</button>
        </div>
        <div style="font-family:var(--mono);font-size:10px;color:var(--text-dim);margin-top:3px">${entry.table || ''}${entry.sigungu ? ' · ' + entry.sigungu : ''}</div>
      </div>`;
  }).join('');
}

async function applyIndexMap(entryJson) {
  const entry = JSON.parse(entryJson);
  // 셀렉트에 값 적용
  document.getElementById('map-div').value = entry.div || '';
  onMapDivChange();
  await new Promise(r => setTimeout(r, 50));
  document.getElementById('map-facility').value = entry.facility || '';
  onMapFacilityChange();
  document.getElementById('map-sido').value = entry.sido || '';
  await onMapSidoChange();
  await new Promise(r => setTimeout(r, 200));
  const sigEl = document.getElementById('map-sigungu');
  sigEl.value = (entry.sigungu || '').replace(/%$/, ''); // % 제거해서 표시
  updateTableLabel();
  // 자동 조회
  await loadMapData();
}

async function deleteIndexMap(name) {
  try {
    const res  = await fetch(`${window.flaskUrl}/api/map/index/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    const data = await res.json();
    renderIndexMapList(data.index_map);
    addLog('info', `인덱스맵 삭제: ${name}`);
  } catch(e) {}
}

// ── 포인트 추출 (3D 관로 전용) ──────────────────────────
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
    const filtered = Object.entries(mapMeta.facilities[div].facilities)
      .filter(([k]) => k.endsWith('_3D'));
    filtered.forEach(([k, v]) => {
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

  // 로그 탭으로 이동해서 진행상황 표시
  const logNav = document.querySelector('.nav-item[onclick*="log"]');
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

      buffer = lines.pop(); // 마지막 불완전한 청크 보관

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


// ── 페이지 전환 시 ───────────────────────────────────────
function onMapPageShow() {
  if (!olMap) {
    initMap();
    loadMapMeta();
  } else {
    setTimeout(() => olMap.updateSize(), 100);
  }
}

// ── window 노출 ──────────────────────────────────────────
window.loadMapData     = loadMapData;
window.mapFitExtent    = mapFitExtent;
window.closeMapPopup   = closeMapPopup;
window.toggleLayer     = toggleLayer;
window.onMapPageShow   = onMapPageShow;
window.onMapDivChange  = onMapDivChange;
window.onMapFacilityChange = onMapFacilityChange;
window.onMapSidoChange = onMapSidoChange;
window.openSaveIndexMap = openSaveIndexMap;
window.saveIndexMap    = saveIndexMap;
window.applyIndexMap   = applyIndexMap;
window.deleteIndexMap  = deleteIndexMap;
window.onPtDivChange   = onPtDivChange;
window.updatePtTableLabel = updatePtTableLabel;
window.extractPoints   = extractPoints;