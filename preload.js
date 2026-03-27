const { contextBridge, ipcRenderer } = require('electron');

// ─── 렌더러에 안전하게 노출할 API 정의 ────────────────────────
// window.electronAPI 로 렌더러에서 접근 가능
contextBridge.exposeInMainWorld('electronAPI', {

  // ── Flask 서버 URL 가져오기 ──────────────────────────────────
  // 사용: const url = await window.electronAPI.getFlaskUrl();
  getFlaskUrl: () => ipcRenderer.invoke('flask-url'),

  // ── Python API 호출 헬퍼 ─────────────────────────────────────
  // 사용: const data = await window.electronAPI.fetchApi('/api/hello');
  fetchApi: async (endpoint, options = {}) => {
    const baseUrl = await ipcRenderer.invoke('flask-url');
    const res = await fetch(`${baseUrl}${endpoint}`, {
      headers: { 'Content-Type': 'application/json', ...options.headers },
      ...options,
    });
    if (!res.ok) throw new Error(`API 오류: ${res.status} ${res.statusText}`);
    return res.json();
  },

  // ── 앱 정보 ──────────────────────────────────────────────────
  // 사용: const info = window.electronAPI.appInfo;
  appInfo: {
    platform: process.platform,   // 'win32' | 'darwin' | 'linux'
    version: process.env.npm_package_version ?? '1.0.0',
  },

  // ── 메인 프로세스로 로그 전송 (선택) ────────────────────────
  // 사용: window.electronAPI.log('info', '메시지');
  log: (level, message) => ipcRenderer.send('log', { level, message }),

});

// ─── 렌더러 로드 완료 시 콘솔 확인용 ────────────────────────
console.log('[Preload] electronAPI 로드 완료');