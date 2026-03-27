from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import os
import re
import geopandas as gpd
from sqlalchemy import create_engine, text

load_dotenv()

app = Flask(__name__)
CORS(app)

FLASK_PORT = 5000


# ─── DB 연결 ──────────────────────────────────────────────
def get_connection():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
    )
    # DB 인코딩이 EUC-KR인 경우 Python이 UTF-8로 자동 변환하도록 설정
    conn.set_client_encoding('UTF8')
    return conn


# ─── 테이블명 유효성 검사 (SQL Injection 방지) ────────────────
def is_valid_identifier(name):
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_\.]{0,127}$', name))



# ─── PK 컬럼 조회 헬퍼 ───────────────────────────────────
def get_pk_columns(conn, table):
    """테이블의 PK 컬럼 목록 반환 (schema.table 형식 지원)"""
    parts = table.split('.')
    schema = parts[0] if len(parts) == 2 else 'public'
    tname  = parts[1] if len(parts) == 2 else parts[0]
    cur = conn.cursor()
    cur.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema    = %s
          AND tc.table_name      = %s
        ORDER BY kcu.ordinal_position
    """, (schema, tname))
    pks = [r[0] for r in cur.fetchall()]
    cur.close()
    return pks


# ─── 헬스체크 ─────────────────────────────────────────────
@app.route('/api/health')
def health():
    return jsonify({'status': 'ok'})


# ─── 테이블 조회 ──────────────────────────────────────────
@app.route('/api/query', methods=['POST'])
def query_table():
    data = request.get_json()

    table   = (data.get('table')   or '').strip()
    column  = (data.get('column')  or '').strip()
    value   = (data.get('value')   or '').strip()
    column2 = (data.get('column2') or '').strip()
    value2  = (data.get('value2')  or '').strip()

    # 입력값 검증
    if not table:
        return jsonify({'error': '테이블명을 입력해주세요.'}), 400
    if not is_valid_identifier(table):
        return jsonify({'error': '유효하지 않은 테이블명입니다.'}), 400
    if column and not is_valid_identifier(column):
        return jsonify({'error': '유효하지 않은 WHERE 컬럼명입니다.'}), 400
    if column2 and not is_valid_identifier(column2):
        return jsonify({'error': '유효하지 않은 AND 컬럼명입니다.'}), 400

    try:
        conn = get_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        params = []

        # WHERE 절 조건 조합
        def make_cond(col, val):
            """컬럼=값 또는 컬럼 LIKE 값 조건 반환"""
            if '%' in val or '_' in val:
                return f'{col} LIKE %s'
            return f'{col} = %s'

        if column and value and column2 and value2:
            # WHERE col1 ... AND col2 ...
            cond1 = make_cond(column, value)
            cond2 = make_cond(column2, value2)
            sql = f'SELECT * FROM {table} WHERE {cond1} AND {cond2} LIMIT 500'
            params = [value, value2]

        elif column and value:
            # WHERE col1 ...
            cond1 = make_cond(column, value)
            sql = f'SELECT * FROM {table} WHERE {cond1} LIMIT 500'
            params = [value]

        else:
            # 조건 없이 전체 조회
            sql = f'SELECT * FROM {table} LIMIT 500'

        cur.execute(sql, params)

        rows    = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        pks = get_pk_columns(conn, table)  # conn 닫기 전에 먼저 조회
        cur.close()
        conn.close()

        return jsonify({
            'columns':    columns,
            'rows':       [dict(row) for row in rows],
            'count':      len(rows),
            'pk_columns': pks,
        })

    except psycopg2.OperationalError as e:
        return jsonify({'error': f'DB 연결 실패: {str(e)}'}), 503
    except psycopg2.errors.UndefinedTable:
        return jsonify({'error': f'테이블을 찾을 수 없습니다: {table}'}), 404
    except psycopg2.errors.UndefinedColumn:
        return jsonify({'error': f'컬럼을 찾을 수 없습니다'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── 셀 업데이트 (커밋) ──────────────────────────────────
@app.route('/api/update', methods=['POST'])
def update_rows():
    data    = request.get_json()
    table   = (data.get('table')   or '').strip()
    updates = data.get('updates') or []   # [{column, value, pk_map}, ...]

    if not table:
        return jsonify({'error': '테이블명이 없습니다.'}), 400
    if not is_valid_identifier(table):
        return jsonify({'error': '유효하지 않은 테이블명입니다.'}), 400
    if not updates:
        return jsonify({'error': '업데이트 항목이 없습니다.'}), 400

    try:
        conn = get_connection()
        cur  = conn.cursor()
        updated = 0

        for item in updates:
            col    = (item.get('column') or '').strip()
            val    = item.get('value')
            pk_map = item.get('pk_map') or {}

            if not col or not pk_map:
                continue
            if not is_valid_identifier(col):
                conn.rollback(); conn.close()
                return jsonify({'error': f'유효하지 않은 컬럼명: {col}'}), 400
            for pk_col in pk_map:
                if not is_valid_identifier(pk_col):
                    conn.rollback(); conn.close()
                    return jsonify({'error': f'유효하지 않은 PK 컬럼명: {pk_col}'}), 400

            # SET col = %s WHERE pk1 = %s AND pk2 = %s ...
            where_clause = ' AND '.join([f'{pk} = %s' for pk in pk_map])
            sql    = f'UPDATE {table} SET {col} = %s WHERE {where_clause}'
            params = [val] + list(pk_map.values())
            cur.execute(sql, params)
            updated += cur.rowcount

        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'updated': updated})

    except psycopg2.OperationalError as e:
        return jsonify({'error': f'DB 연결 실패: {str(e)}'}), 503
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({'error': str(e)}), 500


# ─── SQLAlchemy 엔진 (geopandas용) ───────────────────────
def get_engine():
    host     = os.getenv('DB_HOST', 'localhost')
    port     = os.getenv('DB_PORT', '5432')
    dbname   = os.getenv('DB_NAME')
    user     = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    return create_engine(
        f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}',
        connect_args={'client_encoding': 'utf8'},
    )


# ─── SHP 다운로드 ──────────────────────────────────────────
@app.route('/api/export/shp', methods=['POST'])
def export_shp():
    data    = request.get_json()
    table   = (data.get('table')   or '').strip()
    column  = (data.get('column')  or '').strip()
    value   = (data.get('value')   or '').strip()
    column2 = (data.get('column2') or '').strip()
    value2  = (data.get('value2')  or '').strip()

    if not table:
        return jsonify({'error': '테이블명을 입력해주세요.'}), 400
    if not is_valid_identifier(table):
        return jsonify({'error': '유효하지 않은 테이블명입니다.'}), 400

    # ── WHERE 절 조합 ──────────────────────────────────────
    def make_cond(col, val):
        if '%' in val or '_' in val:
            return f"{col} LIKE '{val}'"
        return f"{col} = '{val}'"

    conditions = []
    if column and value:
        conditions.append(make_cond(column, value))
    if column2 and value2:
        conditions.append(make_cond(column2, value2))

    where   = f"WHERE {' AND '.join(conditions)}" if conditions else ''

    # ── geopandas로 읽기 ───────────────────────────────────
    # ST_AsText(WKT) 방식 — WKB 파싱 오류 없이 모든 PostGIS 버전 호환
    sql_geo = (
        f"SELECT *, ST_AsText(geom) AS geom_wkt, ST_SRID(geom) AS geom_srid "
        f"FROM {table} {where}"
    )
    try:
        import pandas as pd
        from shapely import wkt as shapely_wkt

        engine = get_engine()
        with engine.connect() as con:
            df = pd.read_sql(text(sql_geo), con)

        if df.empty:
            return jsonify({'error': '조건에 맞는 데이터가 없습니다.'}), 404

        # WKT → shapely geometry 변환
        df['geometry'] = df['geom_wkt'].apply(
            lambda w: shapely_wkt.loads(w) if w else None
        )
        # 원본 geom 관련 컬럼 제거
        df = df.drop(columns=[c for c in ['geom', 'geom_wkt', 'geom_srid'] if c in df.columns])

        gdf = gpd.GeoDataFrame(df, geometry='geometry')

    except Exception as e:
        import traceback
        return jsonify({'error': f'공간 데이터 읽기 실패: {str(e)}', 'detail': traceback.format_exc()}), 500


    # ── 좌표계 설정 (EPSG:5186) ────────────────────────────
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=5186)
    else:
        gdf = gdf.to_crs(epsg=5186)

    # ── 저장 경로: C:\TEMP\SHP\ ────────────────────────────
    out_dir  = r'C:\TEMP\SHP'
    os.makedirs(out_dir, exist_ok=True)

    # 테이블명에서 schema 접두사 제거 (schema.tablename → tablename)
    shp_name = table.split('.')[-1]
    shp_path = os.path.join(out_dir, f'{shp_name}.shp')

    # ── 컬럼명 10자 제한 (DBF 스펙) ───────────────────────
    rename_map = {}
    for col in gdf.columns:
        if col == gdf.geometry.name:
            continue
        if len(col) > 10:
            rename_map[col] = col[:10]
    if rename_map:
        gdf = gdf.rename(columns=rename_map)

    # ── SHP 저장 (EUC-KR 인코딩) ──────────────────────────
    try:
        gdf.to_file(shp_path, driver='ESRI Shapefile', encoding='euc-kr')
    except Exception as e:
        return jsonify({'error': f'SHP 저장 실패: {str(e)}'}), 500

    return jsonify({
        'message': f'저장 완료: {shp_path}',
        'path':    shp_path,
        'count':   len(gdf),
        'renamed': rename_map,
    })



# ─── SHP 히스토리 파일 경로 ──────────────────────────────
SHP_HISTORY_FILE = os.path.join(os.path.dirname(__file__), 'shp_history.json')

def load_shp_history():
    try:
        if os.path.exists(SHP_HISTORY_FILE):
            import json as _json
            with open(SHP_HISTORY_FILE, 'r', encoding='utf-8') as f:
                return _json.load(f)
    except Exception:
        pass
    return []

def save_shp_history(history):
    import json as _json
    with open(SHP_HISTORY_FILE, 'w', encoding='utf-8') as f:
        _json.dump(history, f, ensure_ascii=False, indent=2)


# ─── SHP 목록 조회 ───────────────────────────────────────
@app.route('/api/service/shp-list', methods=['GET'])
def list_shp_files():
    shp_dir = r'C:\TEMP\SHP'
    if not os.path.exists(shp_dir):
        return jsonify({'files': [], 'dir': shp_dir, 'history': []})
    files = [f for f in os.listdir(shp_dir) if f.lower().endswith('.shp')]
    files.sort()
    return jsonify({'files': files, 'dir': shp_dir, 'history': load_shp_history()})


# ─── SHP 히스토리 추가 ───────────────────────────────────
@app.route('/api/service/shp-history/add', methods=['POST'])
def add_shp_history():
    data     = request.get_json()
    shp_file = (data.get('shp_file') or '').strip()
    if not shp_file:
        return jsonify({'error': '파일명 없음'}), 400
    history = load_shp_history()
    # 중복 제거 후 맨 앞에 추가, 최대 5개
    history = [h for h in history if h != shp_file]
    history.insert(0, shp_file)
    history = history[:5]
    save_shp_history(history)
    return jsonify({'history': history})


# ─── SHP 히스토리 삭제 ───────────────────────────────────
@app.route('/api/service/shp-history/delete', methods=['POST'])
def delete_shp_history():
    data     = request.get_json()
    shp_file = (data.get('shp_file') or '').strip()
    history  = load_shp_history()
    history  = [h for h in history if h != shp_file]
    save_shp_history(history)
    return jsonify({'history': history})


# ─── 서비스 파일 찾기 + JSON 수정 + 실행 ──────────────────
@app.route('/api/service/build', methods=['POST'])
def build_service():
    import json, subprocess, traceback

    data     = request.get_json()
    table    = (data.get('table')    or '').strip()
    shp_file = (data.get('shp_file') or '').strip()   # wtl_valv_ps_11000.shp
    dem_code = (data.get('dem_code') or '').strip()   # 11110

    if not table:
        return jsonify({'error': '테이블명을 입력해주세요.'}), 400
    if not shp_file:
        return jsonify({'error': 'SHP 파일을 선택해주세요.'}), 400
    if not dem_code:
        return jsonify({'error': 'DEM 코드를 입력해주세요.'}), 400

    # ── 테이블명에서 숫자 코드 제거 → 기본명 ─────────────────
    base_name = re.sub(r'_\d+$', '', table.upper())
    if '.' in base_name:
        base_name = base_name.split('.')[-1]

    # ── JSON 파일 경로 ────────────────────────────────────────
    param_dir    = os.getenv('SERVICE_PARAM_DIR', '')
    json_filename = f'BuildInstancedModel_{base_name}_3D.json'
    json_path     = os.path.join(param_dir, json_filename)

    if not os.path.exists(json_path):
        return jsonify({'error': f'JSON 파일 없음: {json_filename}', 'searched': json_path}), 404

    # ── JSON 읽기 (인코딩 자동 감지) ────────────────────────────
    cfg = None
    for enc in ('utf-8', 'utf-8-sig', 'euc-kr', 'cp949', 'latin-1'):
        try:
            with open(json_path, 'r', encoding=enc) as f:
                cfg = json.load(f)
            break
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    if cfg is None:
        return jsonify({'error': f'JSON 읽기 실패: 지원하지 않는 인코딩이거나 유효하지 않은 JSON입니다.'}), 500

    # ── 내용 수정 ─────────────────────────────────────────────
    shp_full_path = f'C:/TEMP/SHP/{shp_file}'.replace('\\', '/')

    def patch(obj):
        """재귀적으로 dict/list 탐색하며 값 교체"""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == 'InputDemFilePath':
                    obj[k] = f'E:/g/{dem_code[:2]}000/{dem_code}.tif'
                elif k == 'DataSourceConnection':
                    obj[k] = shp_full_path
                else:
                    patch(v)
        elif isinstance(obj, list):
            for item in obj:
                patch(item)
    patch(cfg)

    # ── 수정된 JSON 저장 (원본 인코딩 유지) ─────────────────────
    save_enc = 'utf-8'
    for enc in ('utf-8', 'utf-8-sig', 'euc-kr', 'cp949'):
        try:
            with open(json_path, 'r', encoding=enc) as f:
                f.read()
            save_enc = enc
            break
        except UnicodeDecodeError:
            continue
    try:
        with open(json_path, 'w', encoding=save_enc) as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:
        return jsonify({'error': f'JSON 저장 실패: {e}'}), 500

    # ── ThreeDTilesBuilderCUI.exe 실행 ──────────────────────
    # .env: SERVICE_BUILDER_EXE=D:\ThreeDTilesBuilder\ThreeDTilesBuilderCUI.exe
    runner_exe = os.getenv('SERVICE_BUILDER_EXE', '')
    if not runner_exe:
        return jsonify({'error': '.env에 SERVICE_BUILDER_EXE 가 설정되지 않았습니다.'}), 500
    if not os.path.exists(runner_exe):
        return jsonify({'error': f'실행 파일 없음: {runner_exe}'}), 500

    try:
        # ThreeDTilesBuilderCUI.exe [json파일경로] 형태로 실행
        proc = subprocess.Popen(
            [runner_exe, json_path],
            cwd=os.path.dirname(runner_exe),
            creationflags=subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0,
        )
        # 히스토리 저장
        hist = load_shp_history()
        hist = [h for h in hist if h != shp_file]
        hist.insert(0, shp_file)
        save_shp_history(hist[:5])

        return jsonify({
            'message':   f'빌더 실행 시작 (PID: {proc.pid})',
            'json_path': json_path,
            'dem':       f'E:/g/{dem_code[:2]}000/{dem_code}.tif',
            'shp':       shp_full_path,
            'pid':       proc.pid,
        })
    except Exception as e:
        return jsonify({'error': f'빌더 실행 실패: {e}', 'detail': traceback.format_exc()}), 500




# ─── 에러 핸들러 ──────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return jsonify({'error': '존재하지 않는 엔드포인트입니다'}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({'error': '서버 내부 오류가 발생했습니다'}), 500


# ─── Blueprint 등록 (모든 함수 정의 이후) ──────────────────
from map_routes import map_bp, init_map_routes
init_map_routes(get_engine, is_valid_identifier)
app.register_blueprint(map_bp)


# ─── 서버 실행 ────────────────────────────────────────────
if __name__ == '__main__':
    print(f'[Flask] 서버 시작: http://localhost:{FLASK_PORT}')
    app.run(host='127.0.0.1', port=FLASK_PORT, debug=False)