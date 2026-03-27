from flask import Blueprint, jsonify, request
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