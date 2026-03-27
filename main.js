const { app, BrowserWindow, ipcMain } = require('electron');
const { spawn } = require('child_process');
const path = require('path');

// ─── 설정 ───────────────────────────────────────────────
const FLASK_PORT = 5000;
const FLASK_HOST = `http://localhost:${FLASK_PORT}`;
let pythonProcess = null;
let mainWindow = null;

// ─── Python(Flask) 서버 시작 ─────────────────────────────
function startPythonServer() {
  const fs = require('fs');

  // 개발: __dirname 사용, 배포(패키징): process.resourcesPath 사용
  const isDev     = !app.isPackaged;
  const pythonDir = isDev
    ? path.join(__dirname, 'python')
    : path.join(process.resourcesPath, 'python');

  const scriptPath = path.join(pythonDir, 'app.py');

  const venvPython = process.platform === 'win32'
    ? path.join(pythonDir, 'venv', 'Scripts', 'python.exe')
    : path.join(pythonDir, 'venv', 'bin', 'python');

  const pythonCmd = fs.existsSync(venvPython)
    ? venvPython
    : (process.platform === 'win32' ? 'python' : 'python3');

  console.log(`[Electron] isDev: ${isDev}`);
  console.log(`[Electron] Python 실행: ${pythonCmd}`);
  console.log(`[Electron] 스크립트: ${scriptPath}`);

  pythonProcess = spawn(pythonCmd, [scriptPath], {
    cwd: pythonDir,
  });

  // 로그 파일 경로 (배포 환경에서 디버깅용)
  const logDir  = app.getPath('userData');
  const logFile = path.join(logDir, 'python.log');
  const fs2     = require('fs');
  const writeLog = (msg) => {
    const line = `[${new Date().toISOString()}] ${msg}
`;
    fs2.appendFileSync(logFile, line, 'utf8');
    console.log(msg);
  };

  writeLog(`[Python] 실행 경로: ${pythonCmd}`);
  writeLog(`[Python] 스크립트: ${scriptPath}`);
  writeLog(`[Python] 작업 디렉토리: ${pythonDir}`);

  pythonProcess.stdout.on('data', (data) => {
    writeLog(`[Python] ${data.toString().trim()}`);
  });

  pythonProcess.stderr.on('data', (data) => {
    writeLog(`[Python] ${data.toString().trim()}`);
  });

  pythonProcess.on('close', (code) => {
    writeLog(`[Python] 프로세스 종료 (코드: ${code})`);
  });

  pythonProcess.on('error', (err) => {
    writeLog(`[Python] 실행 실패: ${err.message}`);
  });
}

// ─── Flask 서버가 준비될 때까지 대기 ────────────────────────
function waitForFlask(retries = 20, delay = 500) {
  return new Promise((resolve, reject) => {
    const http = require('http');
    let attempts = 0;

    function check() {
      http.get(`${FLASK_HOST}/api/health`, (res) => {
        if (res.statusCode === 200) {
          resolve();
        } else {
          retry();
        }
      }).on('error', retry);
    }

    function retry() {
      attempts++;
      if (attempts >= retries) {
        reject(new Error('Flask 서버 시작 실패 (10초 초과)'));
      } else {
        setTimeout(check, delay);
      }
    }

    check();
  });
}

// ─── 메인 윈도우 생성 ─────────────────────────────────────
function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    minWidth: 800,
    minHeight: 600,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
    show: false,
  });

  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'));

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
    mainWindow.focus();
    mainWindow.webContents.focus();
  });

  if (process.env.NODE_ENV === 'development') {
    mainWindow.webContents.openDevTools();
  }

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

// ─── IPC 핸들러 ──────────────────────────────────────────
ipcMain.handle('flask-url', () => FLASK_HOST);

// ─── 앱 생명주기 ──────────────────────────────────────────
app.whenReady().then(async () => {
  // 1) Python 서버 시작
  startPythonServer();

  try {
    // 2) Flask 응답 대기 (최대 10초)
    await waitForFlask();
    console.log('[Electron] Flask 서버 준비 완료');
  } catch (err) {
    console.error('[Electron] Flask 서버 연결 실패:', err.message);
  }

  // 3) 창 생성
  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

// 앱 종료 시 Python도 함께 종료
app.on('will-quit', () => {
  if (pythonProcess) {
    // Windows는 kill()로 안 죽는 경우가 있어 taskkill 사용
    if (process.platform === 'win32') {
      spawn('taskkill', ['/pid', pythonProcess.pid, '/f', '/t']);
    } else {
      pythonProcess.kill();
    }
    console.log('[Electron] Python 프로세스 종료');
  }
});