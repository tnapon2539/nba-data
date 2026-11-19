#!/usr/bin/env python3
"""NBA data collector v2 — หลายแหล่งข้อมูล + ตรวจสุขภาพแหล่ง + status.json (ใช้ไลบรารีมาตรฐานของ Python เท่านั้น)

แหล่งข้อมูล
  หลัก : cdn.nba.com   (board / schedule / boxscore ย้อนหลัง)
  สำรอง: site.api.espn.com (board / ผลย้อนหลังรายวัน+boxscore / injuries)
  * ไม่มีแหล่งใดรับประกันว่าจะเข้าถึงได้ตลอด — ผลการเข้าถึงจริงทุกครั้งบันทึกไว้ใน status.json
  * ราคาต่อรอง (odds) ไม่อยู่ในสคริปต์นี้ — ดึงผ่าน Odds API ใน Apps Script (ตั้ง ODDS_API_KEY)

ไฟล์ผลลัพธ์ใน --out
  games.csv, player_logs.csv   ข้อมูลย้อนหลัง (คอลัมน์ตรงกับชีต Games / PlayerLogs)
  schedule.json                ตารางแข่งฤดูกาลปัจจุบัน (ดิบจาก NBA; ไม่มีเมื่อ NBA ถูกบล็อก)
  board.json                   สกอร์/สถานะวันนี้ในรูปแบบ NBA-like + _feed{source,fetched_at}
  injuries.json                รายงานบาดเจ็บจาก ESPN (ไม่ใช่ทางการ — อาจไม่ครบ/ล่าช้า)
  status.json                  สุขภาพแต่ละแหล่ง + เวลาดึงล่าสุดของแต่ละไฟล์ + คำเตือนตรวจสอบข้อมูล

ใช้:  python fetch_nba.py --out data --seasons 3
      python fetch_nba.py --out data --board-only
Exit code: 0 สำเร็จ | 2 ทุกแหล่งที่จำเป็นใช้ไม่ได้ (ถูกบล็อก/ล่ม)
"""
import argparse, csv, datetime as dt, json, os, re, sys, time, unicodedata, urllib.error, urllib.request
from concurrent.futures import ThreadPoolExecutor

CDN = 'https://cdn.nba.com/static/json'
ESPN = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
           'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'en-US,en;q=0.9',
           'Referer': 'https://www.nba.com/', 'Origin': 'https://www.nba.com'}
GAME_H = ['eventId', 'date', 'season', 'homeId', 'homeAbbr', 'awayId', 'awayAbbr', 'homeScore', 'awayScore', 'status', 'homeSpread', 'total']
PLAYER_H = ['eventId', 'date', 'teamAbbr', 'playerId', 'name', 'starter', 'min', 'pts', 'reb', 'ast', 'stl', 'blk', 'tov', 'pf', 'fg3m']

# (tricode, NBA teamId, ชื่อเต็ม)
TEAMS = [
    ('ATL', 1610612737, 'Atlanta Hawks'), ('BOS', 1610612738, 'Boston Celtics'), ('BKN', 1610612751, 'Brooklyn Nets'),
    ('CHA', 1610612766, 'Charlotte Hornets'), ('CHI', 1610612741, 'Chicago Bulls'), ('CLE', 1610612739, 'Cleveland Cavaliers'),
    ('DAL', 1610612742, 'Dallas Mavericks'), ('DEN', 1610612743, 'Denver Nuggets'), ('DET', 1610612765, 'Detroit Pistons'),
    ('GSW', 1610612744, 'Golden State Warriors'), ('HOU', 1610612745, 'Houston Rockets'), ('IND', 1610612754, 'Indiana Pacers'),
    ('LAC', 1610612746, 'LA Clippers'), ('LAL', 1610612747, 'Los Angeles Lakers'), ('MEM', 1610612763, 'Memphis Grizzlies'),
    ('MIA', 1610612748, 'Miami Heat'), ('MIL', 1610612749, 'Milwaukee Bucks'), ('MIN', 1610612750, 'Minnesota Timberwolves'),
    ('NOP', 1610612740, 'New Orleans Pelicans'), ('NYK', 1610612752, 'New York Knicks'), ('OKC', 1610612760, 'Oklahoma City Thunder'),
    ('ORL', 1610612753, 'Orlando Magic'), ('PHI', 1610612755, 'Philadelphia 76ers'), ('PHX', 1610612756, 'Phoenix Suns'),
    ('POR', 1610612757, 'Portland Trail Blazers'), ('SAC', 1610612758, 'Sacramento Kings'), ('SAS', 1610612759, 'San Antonio Spurs'),
    ('TOR', 1610612761, 'Toronto Raptors'), ('UTA', 1610612762, 'Utah Jazz'), ('WAS', 1610612764, 'Washington Wizards'),
]
TEAM_ID = {t[0]: t[1] for t in TEAMS}
TEAM_NAME = {t[0]: t[2] for t in TEAMS}
ALIAS = {'GS': 'GSW', 'NY': 'NYK', 'NO': 'NOP', 'SA': 'SAS', 'UTAH': 'UTA', 'WSH': 'WAS', 'PHO': 'PHX', 'BRK': 'BKN', 'CHO': 'CHA', 'NOH': 'NOP'}


class Blocked(Exception):
    pass


def norm_abbr(a):
    a = str(a or '').strip().upper()
    return ALIAS.get(a, a)


def canon(name):
    """ชื่อผู้เล่นแบบ canonical: ตัดสำเนียง/เครื่องหมาย/ช่องว่างซ้ำ เพื่อจับคู่ข้ามแหล่ง (Dončić = Doncic)"""
    s = unicodedata.normalize('NFKD', str(name or ''))
    s = ''.join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[.'’`-]", '', s)
    return re.sub(r'\s+', ' ', s).strip()


def now_iso():
    return dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


# ---------------------------------------------------------------- HTTP
def http_get(url, tries=3):
    last = (0, b'')
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=30) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            body = e.read() if hasattr(e, 'read') else b''
            if e.code in (403, 404):
                return e.code, body
            last = (e.code, body)
        except Exception as e:  # เครือข่ายล่ม ฯลฯ
            last = (0, str(e).encode())
        time.sleep(1.5 * (i + 1))
    return last


def is_blocked(code, body):
    """'Access Denied' แบบ HTML = ถูกบล็อก IP ; XML AccessDenied ของ S3 = ไม่มีไฟล์นั้น (เกมที่ยังไม่แข่ง)"""
    up = (body or b'')[:600].upper()
    return code == 403 and b'<HTML' in up and b'ACCESS DENIED' in up


def get_json(url):
    """คืน (state, json): ok | blocked | missing | error"""
    code, body = http_get(url)
    if code == 200:
        try:
            return 'ok', json.loads(body)
        except ValueError:
            return 'error', None
    if is_blocked(code, body):
        return 'blocked', None
    if code in (403, 404):
        return 'missing', None
    return 'error', None


# ---------------------------------------------------------------- เวลา/ฤดูกาล
def et_today():
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=5)).date()


def season_of(d):
    return d.year + 1 if d.month >= 10 else d.year


def game_id(start_year, n):
    return '002%02d%05d' % (start_year % 100, n)


def iso_minutes(s):
    m = re.match(r'PT(?:(\d+)M)?(?:([\d.]+)S)?', str(s or ''))
    return (int(m.group(1) or 0) + float(m.group(2) or 0) / 60) if m else 0.0


# ---------------------------------------------------------------- NBA parse
def parse_box(j):
    g = (j or {}).get('game')
    if not g or not g.get('homeTeam') or not g.get('awayTeam') or int(g.get('gameStatus') or 0) != 3:
        return None
    date = str(g.get('gameEt') or g.get('gameTimeLocal') or '')[:10]
    if len(date) != 10:
        return None
    y, m = int(date[:4]), int(date[5:7])
    h, a = g['homeTeam'], g['awayTeam']
    game = [str(g['gameId']), date, y + 1 if m >= 10 else y, h['teamId'], norm_abbr(h['teamTricode']), a['teamId'], norm_abbr(a['teamTricode']),
            h.get('score', 0), a.get('score', 0), 'final', '', '']
    players = []
    for t in (h, a):
        for p in t.get('players', []):
            st = p.get('statistics') or {}
            mins = iso_minutes(st.get('minutes'))
            if mins <= 0:
                continue
            name = p.get('name') or ('%s %s' % (p.get('firstName', ''), p.get('familyName', ''))).strip()
            players.append([str(g['gameId']), date, norm_abbr(t['teamTricode']), p.get('personId'), name, 1 if str(p.get('starter')) == '1' else 0,
                            round(mins, 1), st.get('points', 0), st.get('reboundsTotal', 0), st.get('assists', 0), st.get('steals', 0),
                            st.get('blocks', 0), st.get('turnovers', 0), st.get('foulsPersonal', 0), st.get('threePointersMade', 0)])
    return game, players


# ---------------------------------------------------------------- ESPN parse
def _num(x, default=0):
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return default


def espn_scoreboard_games(j, date_iso):
    """คืนรายการเกมจบแล้ว (รอบปกติ) จาก ESPN scoreboard: [{espn_id, home, away, hs, as}]"""
    out = []
    for ev in (j or {}).get('events', []):
        st = ((ev.get('status') or {}).get('type') or {})
        if not st.get('completed'):
            continue
        stype = ((ev.get('season') or {}).get('type'))
        if stype is not None and int(stype) != 2:
            continue
        comp = (ev.get('competitions') or [{}])[0]
        cs = {c.get('homeAway'): c for c in comp.get('competitors', [])}
        if 'home' not in cs or 'away' not in cs:
            continue
        h, a = norm_abbr(cs['home']['team'].get('abbreviation')), norm_abbr(cs['away']['team'].get('abbreviation'))
        if h not in TEAM_ID or a not in TEAM_ID:
            continue
        out.append({'espn_id': str(ev.get('id')), 'home': h, 'away': a, 'hs': _num(cs['home'].get('score')), 'as': _num(cs['away'].get('score')), 'date': date_iso})
    return out


def espn_summary_players(j, game, id_map):
    """แปลง ESPN summary → แถวผู้เล่น (คอลัมน์เดียวกับ PLAYER_H). playerId จับคู่กับ id ของ NBA จากชื่อ canonical ถ้ามี"""
    rows = []
    for t in ((j or {}).get('boxscore') or {}).get('players', []):
        abbr = norm_abbr((t.get('team') or {}).get('abbreviation'))
        for blk in t.get('statistics', []):
            labels = [str(x).upper() for x in blk.get('labels', [])]
            ix = {l: i for i, l in enumerate(labels)}
            for ath in blk.get('athletes', []):
                stats = ath.get('stats') or []
                if ath.get('didNotPlay') or not stats or 'MIN' not in ix:
                    continue
                g = lambda k: stats[ix[k]] if k in ix and ix[k] < len(stats) else 0
                mins = _num(g('MIN'))
                if mins <= 0:
                    continue
                a = ath.get('athlete') or {}
                name = a.get('displayName') or a.get('shortName') or ''
                pid = id_map.get((canon(name), abbr)) or id_map.get(canon(name)) or 'espn:%s' % a.get('id')
                fg3 = str(g('3PT') or '0-0').split('-')[0]
                rows.append([game['eid'], game['date'], abbr, pid, name, 1 if ath.get('starter') else 0, mins, _num(g('PTS')), _num(g('REB')),
                             _num(g('AST')), _num(g('STL')), _num(g('BLK')), _num(g('TO')), _num(g('PF')), _num(fg3)])
    return rows


# ---------------------------------------------------------------- CSV helpers
def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline='', encoding='utf-8') as f:
        r = csv.reader(f)
        next(r, None)
        return [row for row in r if row]


def load_state(out):
    """ดึงสถานะเดิมจาก games.csv/player_logs.csv: id, คีย์ date|home|away, แผนที่ชื่อ→playerId"""
    games = read_csv(os.path.join(out, 'games.csv'))
    ids = {r[0] for r in games}
    keys = {'%s|%s|%s' % (r[1], norm_abbr(r[4]), norm_abbr(r[6])) for r in games if len(r) > 6}
    id_map, ambiguous = {}, set()
    for r in read_csv(os.path.join(out, 'player_logs.csv')):
        if len(r) < 5 or str(r[3]).startswith('espn:'):
            continue
        c = canon(r[4])
        id_map[(c, norm_abbr(r[2]))] = r[3]
        if c in id_map and id_map[c] != r[3]:
            ambiguous.add(c)  # ชื่อซ้ำคนละคน → ห้ามจับคู่ด้วยชื่ออย่างเดียว
        id_map.setdefault(c, r[3])
    for c in ambiguous:
        id_map.pop(c, None)
    return ids, keys, id_map


def open_csv(path, header):
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    f = open(path, 'a', newline='', encoding='utf-8')
    w = csv.writer(f)
    if new:
        w.writerow(header)
    return f, w


# ---------------------------------------------------------------- ตรวจสอบข้อมูล
def validate_game(game, players):
    """คืนรายการคำเตือน: คะแนนผิดปกติ, ทีมไม่รู้จัก, ผลรวมแต้มผู้เล่นไม่ตรงคะแนนทีม"""
    w = []
    eid = game[0]
    hs, as_ = _num(game[7]), _num(game[8])
    if game[4] not in TEAM_ID or game[6] not in TEAM_ID:
        w.append('%s: ทีมไม่รู้จัก %s/%s' % (eid, game[4], game[6]))
    if hs <= 0 or as_ <= 0 or hs == as_ or hs > 200 or as_ > 200:
        w.append('%s: สกอร์ผิดปกติ %s-%s' % (eid, hs, as_))
    if players:
        for abbr, score in ((game[4], hs), (game[6], as_)):
            s = sum(_num(p[7]) for p in players if p[2] == abbr)
            if s and abs(s - score) > 3:
                w.append('%s: แต้มผู้เล่นรวม %s ไม่ตรงสกอร์ทีม %s (%s)' % (eid, s, abbr, score))
    return w


# ---------------------------------------------------------------- ประวัติ: NBA CDN
def history_nba(out, seasons, st, workers=6):
    S = season_of(et_today())
    have, keys, _ = load_state(out)
    gf, gw = open_csv(os.path.join(out, 'games.csv'), GAME_H)
    pf, pw = open_csv(os.path.join(out, 'player_logs.csv'), PLAYER_H)
    added = 0
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for start in range(S - seasons, S):
                last_season = (start == S - 1)
                n, empty_blocks = 1, 0
                while n <= 1230:
                    ids = [game_id(start, k) for k in range(n, min(n + 24, 1231)) if game_id(start, k) not in have]
                    n += 24
                    if not ids:
                        continue
                    results = list(pool.map(lambda i: get_json('%s/liveData/boxscore/boxscore_%s.json' % (CDN, i)), ids))
                    if any(s == 'blocked' for s, _ in results):
                        raise Blocked('NBA CDN บล็อก IP นี้ (Access Denied)')
                    finals = 0
                    for state, j in results:
                        parsed = parse_box(j) if state == 'ok' else None
                        if not parsed:
                            continue
                        game, players = parsed
                        key = '%s|%s|%s' % (game[1], game[4], game[6])
                        if key in keys:  # เกมนี้มีจากแหล่งอื่นแล้ว
                            continue
                        st['warnings'].extend(validate_game(game, players))
                        finals += 1; added += 1
                        have.add(game[0]); keys.add(key)
                        gw.writerow(game); pw.writerows(players)
                    gf.flush(); pf.flush()
                    print('NBA ฤดูกาล %d-%d เกมที่ %d: เพิ่ม %d เกม' % (start, start + 1, n - 24, finals), flush=True)
                    if finals == 0:
                        empty_blocks += 1
                        if last_season or empty_blocks >= 10:
                            break
                    else:
                        empty_blocks = 0
    finally:
        gf.close(); pf.close()
    return added


# ---------------------------------------------------------------- ประวัติ: ESPN (สำรอง)
def history_espn(out, seasons, st, scan_all=False, workers=6):
    today = et_today()
    S = season_of(today)
    have, keys, id_map = load_state(out)
    start = dt.date(S - seasons, 10, 1)
    if keys and not scan_all:  # ต่อจากวันสุดท้ายที่มี (ถอย 3 วันเผื่อเกมตกหล่น)
        last = max(k.split('|')[0] for k in keys)
        start = max(start, dt.date.fromisoformat(last) - dt.timedelta(days=3))
    gf, gw = open_csv(os.path.join(out, 'games.csv'), GAME_H)
    pf, pw = open_csv(os.path.join(out, 'player_logs.csv'), PLAYER_H)
    added, d = 0, start
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            while d <= today:
                diso = d.isoformat()
                state, sb = get_json('%s/scoreboard?dates=%s&limit=300' % (ESPN, d.strftime('%Y%m%d')))
                if state == 'blocked':
                    raise Blocked('ESPN บล็อก IP นี้ (Access Denied)')
                todo = [g for g in (espn_scoreboard_games(sb, diso) if state == 'ok' else [])
                        if '%s|%s|%s' % (diso, g['home'], g['away']) not in keys]
                if todo:
                    sums = list(pool.map(lambda g: get_json('%s/summary?event=%s' % (ESPN, g['espn_id'])), todo))
                    if any(s == 'blocked' for s, _ in sums):
                        raise Blocked('ESPN บล็อก IP นี้ (Access Denied)')
                    for g, (s, sj) in zip(todo, sums):
                        if s != 'ok':
                            st['warnings'].append('ESPN summary %s ไม่สำเร็จ (%s)' % (g['espn_id'], s))
                            continue
                        g['eid'] = 'E' + g['espn_id']
                        players = espn_summary_players(sj, g, id_map)
                        game = [g['eid'], diso, season_of(d), TEAM_ID[g['home']], g['home'], TEAM_ID[g['away']], g['away'], g['hs'], g['as'], 'final', '', '']
                        st['warnings'].extend(validate_game(game, players))
                        gw.writerow(game); pw.writerows(players)
                        keys.add('%s|%s|%s' % (diso, g['home'], g['away'])); added += 1
                    gf.flush(); pf.flush()
                    print('ESPN %s: เพิ่ม %d เกม' % (diso, len(todo)), flush=True)
                d += dt.timedelta(days=1)
    finally:
        gf.close(); pf.close()
    return added


# ---------------------------------------------------------------- Board
def espn_board_to_nba_like(j, fetched):
    """แปลง ESPN scoreboard → รูปแบบ todaysScoreboard ของ NBA (ฟิลด์ที่ Apps Script ใช้)"""
    games = []
    for ev in (j or {}).get('events', []):
        comp = (ev.get('competitions') or [{}])[0]
        cs = {c.get('homeAway'): c for c in comp.get('competitors', [])}
        if 'home' not in cs or 'away' not in cs:
            continue
        st = (ev.get('status') or {})
        t = st.get('type') or {}
        state = t.get('state')  # pre | in | post
        gs = 3 if (t.get('completed') or state == 'post') else (2 if state == 'in' else 1)
        h, a = cs['home'], cs['away']
        ha, aa = norm_abbr(h['team'].get('abbreviation')), norm_abbr(a['team'].get('abbreviation'))
        secs = st.get('clock') or 0
        clock = 'PT%02dM%05.2fS' % (int(secs // 60), secs % 60) if gs == 2 else ''
        games.append({
            'gameId': 'E' + str(ev.get('id')), 'gameStatus': gs, 'gameStatusText': t.get('shortDetail') or t.get('description') or '',
            'period': st.get('period') or 0, 'gameClock': clock, 'gameEt': ev.get('date'), 'gameTimeUTC': ev.get('date'),
            'homeTeam': {'teamId': TEAM_ID.get(ha, 0), 'teamTricode': ha, 'teamName': h['team'].get('shortDisplayName', ''), 'score': _num(h.get('score'))},
            'awayTeam': {'teamId': TEAM_ID.get(aa, 0), 'teamTricode': aa, 'teamName': a['team'].get('shortDisplayName', ''), 'score': _num(a.get('score'))},
        })
    return {'scoreboard': {'gameDate': (j or {}).get('day', {}).get('date', ''), 'games': games}, '_feed': {'source': 'espn', 'fetched_at': fetched}}


def write_json(path, obj):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False)


def fetch_board(out, st):
    """NBA ก่อน → ไม่ได้ค่อย ESPN. คืนชื่อแหล่งที่ใช้ได้ หรือ None"""
    t = now_iso()
    s, j = get_json(CDN + '/liveData/scoreboard/todaysScoreboard_00.json')
    set_source(st, 'nba_cdn', s, 'todaysScoreboard')
    if s == 'ok' and isinstance(j, dict):
        j['_feed'] = {'source': 'nba_cdn', 'fetched_at': t}
        write_json(os.path.join(out, 'board.json'), j)
        set_file(st, 'board.json', 'nba_cdn', t)
        return 'nba_cdn'
    s2, j2 = get_json(ESPN + '/scoreboard')
    set_source(st, 'espn', s2, 'scoreboard')
    if s2 == 'ok' and isinstance(j2, dict):
        write_json(os.path.join(out, 'board.json'), espn_board_to_nba_like(j2, t))
        set_file(st, 'board.json', 'espn', t)
        return 'espn'
    set_file(st, 'board.json', None, t, ok=False)
    return None


def fetch_schedule(out, st):
    t = now_iso()
    for u in ('/staticData/scheduleLeagueV2_1.json', '/staticData/scheduleLeagueV2.json'):
        s, j = get_json(CDN + u)
        if s == 'ok':
            write_json(os.path.join(out, 'schedule.json'), j)
            set_file(st, 'schedule.json', 'nba_cdn', t)
            return True
        if s == 'blocked':
            set_source(st, 'nba_cdn', 'blocked', 'schedule')
            break
    set_file(st, 'schedule.json', None, t, ok=False)
    return False


# ---------------------------------------------------------------- Injuries (ESPN — ไม่ใช่รายงานทางการครบถ้วน)
def parse_espn_injuries(j, fetched):
    rows = []
    for tm in (j or {}).get('injuries', []):
        # บางครั้ง ESPN ใส่ชื่อทีมเต็มที่ระดับ team; ใช้ชื่อเต็มจับคู่ tricode
        name = tm.get('displayName') or ''
        ab = next((k for k, v in TEAM_NAME.items() if v.lower() == name.lower()), None)
        for it in tm.get('injuries', []):
            ath = it.get('athlete') or {}
            team_ab = norm_abbr(((ath.get('team') or {}).get('abbreviation'))) or ab
            if team_ab not in TEAM_ID:
                team_ab = ab
            if team_ab not in TEAM_ID:
                continue
            det = it.get('details') or {}
            rows.append({'teamAbbr': team_ab, 'teamId': TEAM_ID[team_ab], 'player': ath.get('displayName') or '', 'playerKey': canon(ath.get('displayName')),
                         'status': it.get('status') or '', 'detail': (it.get('shortComment') or det.get('type') or det.get('detail') or '')[:240],
                         'updated': it.get('date') or '', 'source': 'espn'})
    return {'_feed': {'source': 'espn', 'fetched_at': fetched, 'official': False}, 'injuries': rows}


def fetch_injuries(out, st):
    t = now_iso()
    s, j = get_json(ESPN + '/injuries')
    set_source(st, 'espn', s, 'injuries')
    if s == 'ok':
        data = parse_espn_injuries(j, t)
        write_json(os.path.join(out, 'injuries.json'), data)
        set_file(st, 'injuries.json', 'espn', t, extra={'count': len(data['injuries'])})
        return len(data['injuries'])
    set_file(st, 'injuries.json', None, t, ok=False)
    return None


# ---------------------------------------------------------------- status.json
def new_status(out):
    old = {}
    p = os.path.join(out, 'status.json')
    if os.path.exists(p):
        try:
            with open(p, encoding='utf-8') as f:
                old = json.load(f)
        except ValueError:
            old = {}
    return {'version': 2, 'generated_at': now_iso(), 'sources': old.get('sources', {}), 'files': old.get('files', {}),
            'counts': old.get('counts', {}), 'warnings': [], 'mode': ''}


def set_source(st, name, state, what=''):
    prev = st['sources'].get(name, {})
    entry = {'state': state, 'ok': state == 'ok', 'checked_at': now_iso(), 'detail': what}
    if state == 'ok':
        entry['last_ok'] = entry['checked_at']
    elif prev.get('last_ok'):
        entry['last_ok'] = prev['last_ok']
    st['sources'][name] = entry


def set_file(st, name, source, t, ok=True, extra=None):
    prev = st['files'].get(name, {})
    e = {'ok': ok, 'source': source or prev.get('source'), 'attempted_at': t}
    if ok:
        e['fetched_at'] = t
    elif prev.get('fetched_at'):
        e['fetched_at'] = prev['fetched_at']  # ไฟล์เก่ายังอยู่ — แสดงอายุจริง
    if extra:
        e.update(extra)
    st['files'][name] = e


def count_rows(out, st):
    st['counts'] = {'games': len(read_csv(os.path.join(out, 'games.csv'))), 'player_rows': len(read_csv(os.path.join(out, 'player_logs.csv')))}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default='data')
    ap.add_argument('--seasons', type=int, default=3)
    ap.add_argument('--board-only', action='store_true')
    ap.add_argument('--espn-scan-all', action='store_true', help='ESPN: สแกนทุกวันตั้งแต่ต้นช่วง (ช้า) แทนต่อจากวันสุดท้าย')
    a = ap.parse_args(argv)
    os.makedirs(a.out, exist_ok=True)
    st = new_status(a.out)
    st['mode'] = 'board' if a.board_only else 'history'
    code = 0
    try:
        board_src = fetch_board(a.out, st)
        if not a.board_only:
            fetch_schedule(a.out, st)
            fetch_injuries(a.out, st)
            seasons = max(1, min(5, a.seasons))
            added, used = 0, None
            try:
                added = history_nba(a.out, seasons, st)
                used = 'nba_cdn'
                set_source(st, 'nba_cdn', 'ok', 'boxscore history')
            except Blocked as e:
                set_source(st, 'nba_cdn', 'blocked', 'boxscore history')
                st['warnings'].append('NBA CDN ถูกบล็อก → สลับไปใช้ ESPN สำหรับข้อมูลย้อนหลัง (%s)' % e)
                try:
                    added = history_espn(a.out, seasons, st, a.espn_scan_all)
                    used = 'espn'
                    set_source(st, 'espn', 'ok', 'history')
                except Blocked as e2:
                    set_source(st, 'espn', 'blocked', 'history')
                    st['warnings'].append('ESPN ถูกบล็อกด้วย: %s' % e2)
            st['history_source'] = used
            st['counts_added'] = added
            if used is None:
                code = 2
            print('เสร็จ แหล่งย้อนหลัง=%s เพิ่มเกมใหม่ %d เกม' % (used, added))
        elif board_src is None:
            code = 2
        count_rows(a.out, st)
    finally:
        st['warnings'] = st['warnings'][:60]
        write_json(os.path.join(a.out, 'status.json'), st)
    if code:
        print('ผิดพลาด: ทุกแหล่งข้อมูลที่ใช้ได้ถูกบล็อก/ล่ม — ดู status.json', file=sys.stderr)
    return code


if __name__ == '__main__':
    sys.exit(main())
