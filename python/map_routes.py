from flask import Blueprint, jsonify, request, Response, stream_with_context
from sqlalchemy import text
import json as _json
import traceback
import os

map_bp = Blueprint('map', __name__)

# ─── 공통 유틸 (app.py에서 주입받음) ───────────────────────
# app.py에서 register_blueprint 전에 init_map_routes(engine_fn, valid_fn) 호출
_get_engine_fn       = None
_is_valid_id_fn      = None

def init_map_routes(get_engine_fn, is_valid_identifier_fn):
    global _get_engine_fn, _is_valid_id_fn
    _get_engine_fn  = get_engine_fn
    _is_valid_id_fn = is_valid_identifier_fn

def _get_engine():
    return _get_engine_fn()

def _is_valid(name):
    return _is_valid_id_fn(name)

# ─── 시설물 메타 정의 ─────────────────────────────────────
FACILITY_META = {
    'WTL': {
        'label': '상수',
        'facilities': {
            'VALV_PS':    {'label': '밸브',      'geom': 'point'},
            'PIPE_LM_2D': {'label': '관로(2D)',  'geom': 'line'},
            'PIPE_LM_3D': {'label': '관로(3D)',  'geom': 'line'},
            'MANH_PS':    {'label': '맨홀',      'geom': 'point'},
        }
    },
    'SWL': {
        'label': '하수',
        'facilities': {
            'PIPE_LM_2D': {'label': '관로(2D)',  'geom': 'line'},
            'PIPE_LM_3D': {'label': '관로(3D)',  'geom': 'line'},
            'MANH_PS':    {'label': '맨홀',      'geom': 'point'},
        }
    },
    'UFL': {
        'label': '시설물통합',
        'facilities': {
            # 가스 (G)
            'GPIP_LM_2D': {'label': '가스 관로(2D)', 'geom': 'line'},
            'GPIP_LM_3D': {'label': '가스 관로(3D)', 'geom': 'line'},
            'GVAL_PS':    {'label': '가스 밸브',      'geom': 'point'},
            'GMAN_PS':    {'label': '가스 맨홀',      'geom': 'point'},
            # 전력 (B)
            'BPIP_LM_2D': {'label': '전력 관로(2D)', 'geom': 'line'},
            'BPIP_LM_3D': {'label': '전력 관로(3D)', 'geom': 'line'},
            'BMAN_PS':    {'label': '전력 맨홀',      'geom': 'point'},
            # 통신 (K)
            'KPIP_LS_2D': {'label': '통신 관로(2D)', 'geom': 'line'},
            'KPIP_LS_3D': {'label': '통신 관로(3D)', 'geom': 'line'},
            'KMAN_PS':    {'label': '통신 맨홀',      'geom': 'point'},
            # 난방 (H)
            'HPIP_LM_2D': {'label': '난방 관로(2D)', 'geom': 'line'},
            'HPIP_LM_3D': {'label': '난방 관로(3D)', 'geom': 'line'},
            'HMAN_PS':    {'label': '난방 맨홀',      'geom': 'point'},
        }
    },
}
SIDO_LIST = [
    {'code': '11', 'label': '서울'},
    {'code': '26', 'label': '부산'},
    {'code': '27', 'label': '대구'},
    {'code': '28', 'label': '인천'},
    {'code': '29', 'label': '광주'},
    {'code': '30', 'label': '대전'},
    {'code': '31', 'label': '울산'},
    {'code': '36', 'label': '세종'},
    {'code': '41', 'label': '경기'},
    {'code': '51', 'label': '강원'},
    {'code': '43', 'label': '충북'},
    {'code': '44', 'label': '충남'},
    {'code': '52', 'label': '전북'},
    {'code': '46', 'label': '전남'},
    {'code': '47', 'label': '경북'},
    {'code': '48', 'label': '경남'},
    {'code': '50', 'label': '제주'},
]

INDEX_MAP_FILE = os.path.join(os.path.dirname(__file__), 'index_map.json')


# ─── 메타 조회 ────────────────────────────────────────────
@map_bp.route('/api/map/meta', methods=['GET'])
def map_meta():
    return jsonify({
        'facilities': FACILITY_META,
        'sido':       SIDO_LIST,
    })


# ─── 시군구 목록 조회 ─────────────────────────────────────
@map_bp.route('/api/map/sigungu', methods=['POST'])
def map_sigungu():
    get_engine, is_valid_identifier = _get_deps()
    data     = request.get_json()
    div      = (data.get('div')      or '').strip()   # WTL / SWL
    facility = (data.get('facility') or '').strip()   # VALV_PS / PIPE_LM
    sido     = (data.get('sido')     or '').strip()   # 11

    if not all([div, facility, sido]):
        return jsonify({'sigungu': []})

    table = f"{div}_{facility}_{sido}000"
    if not _is_valid(table):
        return jsonify({'sigungu': []})

    try:
        engine = _get_engine()
        sql = text(f"""
            SELECT DISTINCT LEFT(hjd_cde::text, 5) AS code
            FROM {table}
            ORDER BY code
        """)
        with engine.connect() as con:
            rows = con.execute(sql).fetchall()
        codes = [r[0] for r in rows if r[0]]
        return jsonify({'sigungu': [{'code': c+'%', 'label': c} for c in codes]})
    except Exception as e:
        return jsonify({'sigungu': [], 'error': str(e)})


# ─── 맵 조회 (GeoJSON) ───────────────────────────────────
@map_bp.route('/api/map/geojson', methods=['POST'])
def map_geojson():
    data     = request.get_json()
    table    = (data.get('table')    or '').strip()
    sigungu  = (data.get('sigungu')  or '').strip()  # hjd_cde 조건

    # 직접 테이블명 입력 방식도 지원
    div      = (data.get('div')      or '').strip()
    facility = (data.get('facility') or '').strip()
    sido     = (data.get('sido')     or '').strip()

    # 테이블명 자동 조합
    if not table and div and facility and sido:
        table = f"{div}_{facility}_{sido}000"

    if not table:
        return jsonify({'error': '테이블명을 입력해주세요.'}), 400
    if not _is_valid(table):
        return jsonify({'error': '유효하지 않은 테이블명입니다.'}), 400

    where = ''
    if sigungu:
        where = f"WHERE hjd_cde LIKE '{sigungu}'"

    try:
        engine  = get_engine()
        parts   = table.split('.')
        schema  = parts[0] if len(parts) == 2 else 'public'
        tname   = parts[-1]

        col_sql = text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name   = :tname
              AND table_schema = :schema
              AND column_name != 'geom'
            ORDER BY ordinal_position
        """)
        with engine.connect() as con:
            col_rows  = con.execute(col_sql, {'tname': tname, 'schema': schema}).fetchall()
            prop_cols = [r[0] for r in col_rows]

            cols_sql_str = ', '.join([f'"{c}"' for c in prop_cols]) if prop_cols else "''"
            data_sql = text(f"""
                SELECT {cols_sql_str},
                       ST_AsGeoJSON(ST_Transform(ST_SetSRID(geom, 5186), 4326)) AS geojson_geom,
                       ST_GeometryType(geom) AS geom_type
                FROM {table} {where}
                LIMIT 2000
            """)
            rows = con.execute(data_sql).fetchall()
            keys = list(prop_cols) + ['geojson_geom', 'geom_type']

        features = []
        for row in rows:
            row_dict  = dict(zip(keys, row))
            geom_str  = row_dict.pop('geojson_geom', None)
            geom_type = row_dict.pop('geom_type',    None)
            if not geom_str:
                continue
            props = {k: str(v) if v is not None else None for k, v in row_dict.items()}
            features.append({
                'type':       'Feature',
                'geometry':   _json.loads(geom_str),
                'properties': props,
            })

        geom_type_simple = 'Point'
        if rows:
            gt = rows[0][-1] or ''
            if   'Line'    in gt: geom_type_simple = 'Line'
            elif 'Polygon' in gt: geom_type_simple = 'Polygon'
            elif 'Point'   in gt: geom_type_simple = 'Point'

        return jsonify({
            'type':      'FeatureCollection',
            'features':  features,
            'count':     len(features),
            'geom_type': geom_type_simple,
            'table':     table,
        })

    except Exception as e:
        return jsonify({'error': str(e), 'detail': traceback.format_exc()}), 500


# ─── 인덱스맵 저장 ────────────────────────────────────────
@map_bp.route('/api/map/index/save', methods=['POST'])
def save_index_map():
    data  = request.get_json()
    name  = (data.get('name') or '').strip()
    entry = data.get('entry')  # {div, facility, sido, sigungu, table, label}

    if not name or not entry:
        return jsonify({'error': '이름과 조건이 필요합니다.'}), 400

    index_map = _load_index_map()
    index_map[name] = entry
    _save_index_map(index_map)
    return jsonify({'message': f'저장 완료: {name}', 'index_map': index_map})


# ─── 인덱스맵 목록 조회 ───────────────────────────────────
@map_bp.route('/api/map/index/list', methods=['GET'])
def list_index_map():
    return jsonify({'index_map': _load_index_map()})


# ─── 인덱스맵 삭제 ────────────────────────────────────────
@map_bp.route('/api/map/index/delete', methods=['POST'])
def delete_index_map():
    data = request.get_json()
    name = (data.get('name') or '').strip()
    index_map = _load_index_map()
    if name in index_map:
        del index_map[name]
        _save_index_map(index_map)
    return jsonify({'index_map': index_map})


# ─── 파일 헬퍼 ───────────────────────────────────────────
def _load_index_map():
    try:
        if os.path.exists(INDEX_MAP_FILE):
            with open(INDEX_MAP_FILE, 'r', encoding='utf-8') as f:
                return _json.load(f)
    except Exception:
        pass
    return {}

def _save_index_map(data):
    with open(INDEX_MAP_FILE, 'w', encoding='utf-8') as f:
        _json.dump(data, f, ensure_ascii=False, indent=2)


# ─── 포인트 추출 (3D 관로 → 꼭짓점 + Z값 + DEM값) ────────
# ─── 포인트 추출 (SSE 스트리밍) ──────────────────────────
@map_bp.route('/api/map/extract-points', methods=['POST'])
def extract_points():
    data     = request.get_json()
    div      = (data.get('div')      or '').strip()
    facility = (data.get('facility') or '').strip()
    sido     = (data.get('sido')     or '').strip()
    sigungu  = (data.get('sigungu')  or '').strip()

    if not all([div, facility, sido]):
        return jsonify({'error': '구분, 시설물, 시도를 선택해주세요.'}), 400

    table = f"{div}_{facility}_{sido}000"
    if not _is_valid(table):
        return jsonify({'error': '유효하지 않은 테이블명입니다.'}), 400

    where = f"WHERE hjd_cde LIKE '{sigungu}'" if sigungu else ''

    def generate():
        import geopandas as gpd
        import pandas as pd
        from shapely.geometry import Point
        from shapely import wkt as _wkt
        import rasterio
        from sqlalchemy import text as _text
        import json as _j

        def sse(msg, t='log'):
            return f"data: {_j.dumps({'type': t, 'msg': msg})}\n\n"

        try:
            yield sse(f'[1/4] DB 조회 시작: {table}')
            engine = _get_engine()
            sql = _text(
                f"SELECT *, ST_AsText(ST_Force3D(geom)) AS geom_wkt_3d "
                f"FROM {table} {where}"
            )
            with engine.connect() as con:
                df = pd.read_sql(sql, con)

            if df.empty:
                yield sse('데이터가 없습니다.', 'error')
                return

            yield sse(f'[1/4] DB 조회 완료: {len(df)}개 라인')
            yield sse(f'[2/4] 꼭짓점 추출 중...')

            records   = []
            prop_cols = [c for c in df.columns if c not in ('geom', 'geom_wkt_3d')]
            total     = len(df)

            for idx, (_, row) in enumerate(df.iterrows()):
                wkt_str = row.get('geom_wkt_3d')
                if not wkt_str:
                    continue
                try:
                    geom = _wkt.loads(wkt_str)
                except Exception:
                    continue

                coords = []
                if hasattr(geom, 'geoms'):
                    for part in geom.geoms:
                        if hasattr(part, 'coords'):
                            coords.extend(list(part.coords))
                elif hasattr(geom, 'coords'):
                    coords = list(geom.coords)

                for coord in coords:
                    x, y = coord[0], coord[1]
                    z    = coord[2] if len(coord) > 2 else None
                    rec  = {c: row.get(c) for c in prop_cols}
                    rec['z']        = round(z, 4) if z is not None else None
                    rec['dem']      = None
                    rec['geometry'] = Point(x, y)
                    records.append(rec)

                if (idx + 1) % 1000 == 0:
                    yield sse(f'[2/4] {idx+1}/{total} 라인 → {len(records)}개 포인트')

            if not records:
                yield sse('꼭짓점 없음', 'error')
                return

            yield sse(f'[2/4] 꼭짓점 추출 완료: {len(records)}개')
            gdf = gpd.GeoDataFrame(records, geometry='geometry', crs='EPSG:5186')

            sigungu_code = sigungu.rstrip('%') if sigungu else sido + '000'
            dem_path     = f"E:/g/{sido}000/{sigungu_code}.tif"
            dem_error    = None
            yield sse(f'[3/4] DEM: {dem_path}')

            if os.path.exists(dem_path):
                try:
                    with rasterio.open(dem_path) as dem:
                        gdf_dem    = gdf.to_crs(dem.crs)
                        all_coords = [(g.x, g.y) for g in gdf_dem.geometry]
                        chunk      = 10000
                        for ci in range(0, len(all_coords), chunk):
                            sampled = list(dem.sample(all_coords[ci:ci+chunk]))
                            nodata  = dem.nodata
                            for j, val in enumerate(sampled):
                                v = float(val[0])
                                gdf.at[ci+j, 'dem'] = None if (nodata and abs(v-nodata)<1e-6) else round(v, 4)
                            yield sse(f'[3/4] DEM 샘플링: {min(ci+chunk, len(all_coords))}/{len(all_coords)}')
                    yield sse(f'[3/4] DEM 완료')
                except Exception as e:
                    dem_error = str(e)
                    yield sse(f'[3/4] DEM 오류: {dem_error}', 'warn')
            else:
                dem_error = f'DEM 없음: {dem_path}'
                yield sse(f'[3/4] {dem_error}', 'warn')

            yield sse(f'[4/4] SHP 저장 중...')
            out_dir  = r'C:\TEMP\SHP'
            os.makedirs(out_dir, exist_ok=True)
            shp_name = f"{table}_points.shp"
            shp_path = os.path.join(out_dir, shp_name)

            # ── dep 필드 추가 (dem - z) ──────────────────────
            gdf['dep'] = gdf.apply(
                lambda r: round(float(r['dem']) - float(r['z']), 4)
                if r['dem'] is not None and r['z'] is not None else None,
                axis=1
            )
            yield sse(f'dep 필드 계산 완료 (dem - z)')

            rename_map = {}
            for col in gdf.columns:
                if col == 'geometry': continue
                if len(col) > 10:
                    rename_map[col] = col[:10]
            if rename_map:
                gdf = gdf.rename(columns=rename_map)

            for col in gdf.columns:
                if col == 'geometry': continue
                gdf[col] = gdf[col].apply(
                    lambda v: str(v) if v is not None and not isinstance(v, (int, float, type(None))) else v
                )

            gdf.to_file(shp_path, driver='ESRI Shapefile', encoding='euc-kr')
            yield sse(f'[4/4] 저장 완료: {shp_path} ({len(gdf)}건)')

            if rename_map:
                yield sse(f'컬럼명 축약: {", ".join([f"{o}:{n}" for o,n in rename_map.items()])}', 'warn')

            result = _j.dumps({
                'path': shp_path, 'count': len(gdf),
                'shp_name': shp_name, 'dem_path': dem_path,
                'dem_error': dem_error, 'renamed': rename_map,
            })
            yield sse(result, 'done')

        except Exception as e:
            import traceback as _tb
            yield sse(f'오류: {str(e)}', 'error')
            yield sse(_tb.format_exc(), 'error')

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
    )


# ─── 심도 비교 (추출 포인트 ↔ 맨홀/밸브 테이블) ─────────
@map_bp.route('/api/map/compare-depth', methods=['POST'])
def compare_depth():
    import geopandas as gpd
    import pandas as pd
    from shapely import wkt as _wkt
    from sqlalchemy import text as _text

    data          = request.get_json()
    shp_path      = (data.get('shp_path')      or '').strip()
    facility_type = (data.get('facility_type') or '').strip()  # manhol / valve
    table         = (data.get('table')         or '').strip()

    if not shp_path:
        return jsonify({'error': '추출된 SHP 경로가 없습니다. 먼저 포인트 추출을 실행해주세요.'}), 400
    if not facility_type:
        return jsonify({'error': '유형(맨홀/밸브)을 선택해주세요.'}), 400
    if not table:
        return jsonify({'error': '조회할 테이블명을 입력해주세요.'}), 400
    if not _is_valid(table):
        return jsonify({'error': '유효하지 않은 테이블명입니다.'}), 400
    if not os.path.exists(shp_path):
        return jsonify({'error': f'SHP 파일이 없습니다: {shp_path}'}), 404

    dep_col = 'man_dep' if facility_type == 'manhol' else 'val_dep'

    try:
        # 1. 추출된 포인트 SHP 로드
        pts_gdf = gpd.read_file(shp_path, encoding='euc-kr')
        pts_gdf.columns = [c.strip().lower() for c in pts_gdf.columns]
        if pts_gdf.crs is None:
            pts_gdf = pts_gdf.set_crs(epsg=5186)
        else:
            pts_gdf = pts_gdf.to_crs(epsg=5186)

        if 'dep' not in pts_gdf.columns:
            return jsonify({'error': "추출된 SHP에 'dep' 컬럼이 없습니다."}), 400

        pts_sub = pts_gdf[pts_gdf.geometry.notna()][['dep', 'geometry']].copy()
        pts_sub['dep'] = pd.to_numeric(pts_sub['dep'], errors='coerce')
        pts_sub = pts_sub.rename(columns={'dep': 'dep_extracted'})

        if pts_sub.empty:
            return jsonify({'error': '추출된 포인트가 없습니다.'}), 400

        # 2. DB 테이블에서 위치 + 심도 조회
        engine = _get_engine()
        sql = _text(f"""
            SELECT {dep_col},
                   ST_AsText(ST_Transform(geom, 5186)) AS geom_wkt
            FROM {table}
        """)
        with engine.connect() as con:
            db_df = pd.read_sql(sql, con)

        if db_df.empty:
            return jsonify({'error': '조회된 데이터가 없습니다.'}), 404

        if dep_col not in db_df.columns:
            return jsonify({'error': f"테이블에 '{dep_col}' 컬럼이 없습니다."}), 400

        # 3. GeoDataFrame 생성
        db_df['geometry'] = db_df['geom_wkt'].apply(
            lambda w: _wkt.loads(w) if w else None
        )
        db_gdf = gpd.GeoDataFrame(
            db_df.drop(columns=['geom_wkt']),
            geometry='geometry', crs='EPSG:5186'
        )
        db_gdf = db_gdf[db_gdf.geometry.notna()].reset_index(drop=True)
        db_gdf[dep_col] = pd.to_numeric(db_gdf[dep_col], errors='coerce')

        if db_gdf.empty:
            return jsonify({'error': '유효한 위치 데이터가 없습니다.'}), 400

        # 4. 공간 조인 — 각 DB 행에 가장 가까운 추출 포인트 매칭 (1m 이내)
        TOLERANCE = 1.0  # 단위: 미터 (EPSG:5186)
        joined = db_gdf.sjoin_nearest(
            pts_sub, how='left', distance_col='_dist'
        )

        # 5. 결과 구성
        DEP_TOLERANCE = 0.05  # 심도 일치 허용 오차 (5cm)
        rows = []

        for i, (_, row) in enumerate(joined.iterrows()):
            x = round(row.geometry.x, 4) if row.geometry else None
            y = round(row.geometry.y, 4) if row.geometry else None

            dist    = row.get('_dist')
            dep_db  = row.get(dep_col)
            dep_ext = row.get('dep_extracted') if (dist is not None and not pd.isna(dist) and dist <= TOLERANCE) else None

            try:
                dep_db_f  = float(dep_db)  if dep_db  is not None and not pd.isna(dep_db)  else None
                dep_ext_f = float(dep_ext) if dep_ext is not None and not pd.isna(dep_ext) else None
            except (TypeError, ValueError):
                dep_db_f = dep_ext_f = None

            if dep_db_f is not None and dep_ext_f is not None:
                diff  = round(dep_db_f - dep_ext_f, 4)
                match = abs(diff) <= DEP_TOLERANCE
            else:
                diff  = None
                match = False

            rows.append({
                'no':        i + 1,
                'x':         x,
                'y':         y,
                dep_col:     str(dep_db_f) if dep_db_f is not None else None,
                'dep_ext':   str(dep_ext_f) if dep_ext_f is not None else None,
                'diff':      str(diff) if diff is not None else None,
                'match':     match,
            })

        mismatches = [r for r in rows if not r['match']]

        return jsonify({
            'all':            rows,
            'mismatches':     mismatches,
            'total':          len(rows),
            'mismatch_count': len(mismatches),
            'match_count':    len(rows) - len(mismatches),
            'dep_col':        dep_col,
        })

    except Exception as e:
        return jsonify({'error': str(e), 'detail': traceback.format_exc()}), 500