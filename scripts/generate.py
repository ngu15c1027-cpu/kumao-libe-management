#!/usr/bin/env python3
"""
Morning Dashboard 自動生成スクリプト
毎朝7時(JST)にGitHub Actionsから実行

3事業の直近24時間分のChatworkログ + 各スプレッドシートを分析し data.json を生成する
  - リベ大デンタルクリニック武蔵小杉院
  - リベ大引越センター
  - リベ大オンライン秘書
"""

import os
import io
import csv
import json
import re
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import anthropic

# ============================================================
# 設定
# ============================================================
CHATWORK_TOKEN  = os.environ['CHATWORK_API_TOKEN']
CHATWORK_TOKEN2 = os.environ.get('CHATWORK_API_TOKEN_2', '')  # 2つ目のアカウント（任意）
CLAUDE_API_KEY  = os.environ['CLAUDE_API_KEY']

JST      = timezone(timedelta(hours=9))
CW_BASE  = 'https://api.chatwork.com/v2'
CW_HEADS = {'X-ChatWorkToken': CHATWORK_TOKEN}
DATA_FILE = 'data.json'

# 収益スプレッドシートID
SHEETS = {
    'hikkoshi':      '1qSEMgmQPpZ2BfDf5a2SXcCpS_7O_c5QlkRkAy49MPDY',
    'hisho':         '1rVnSTZ_fnTo6tgQT8S1IVCXFBnXbLQTnSHyMGhtFFiY',
    'musashikosugi': '1c3yPZdER5i4e0syuGkyFabF3cuG7VF_kVZBQ_rNGVL4',
}

# メンバーリストスプレッドシートID
MEMBER_SHEETS = {
    'hikkoshi':      '1-sSB2eYp9qT3Kp5N__SncqnQiLl4b-Mrd68ZLLxijfE',
    'hisho':         '1KDWJkgtxdMzW2sefWNFfU4MM9x0bBhZw5-SeNMQwKtI',
    'musashikosugi': '1vKpPr3zF760EkxTiUpr5h3ZaOet0_l7ap3S2oRIopwY',
}

# 監視Chatworkルーム
ROOMS = {
    'hikkoshi': {
        '313681496': '【引越】リベ大引越センター 運用チャット',
        '357077194': '【引越︰東海】連絡チャット',
        '324266352': '【引越︰関東】連絡チャット',
        '324266360': '【引越：関西】連絡チャット',
        '344798709': '【引越︰九州】連絡チャット',
        '422468835': '【引越：梱包】全体チャット',
        '363471546': '【引越】関東梱包チャット',
        '324456878': '【引越】顧客対応立案チャット',
        '325500151': '【引越：契約後】連絡チャット',
        '325500271': '【引越：完了報告】連絡チャット',
    },
    'hisho': {
        '422224170': '【リベ秘書】フロント陣チャット',
        '425591523': '【リベ秘書】アシディレチャット',
        '370024427': '【リベ秘書】ディレ・プランナー・主担当連携チャット',
        '349769190': '【リベ秘書】3行日報チャット',
        '359972796': '【リベ秘書】インシデント・アクシデント共有チャット',
        '341276884': '【リベ秘書】ざつだん・運用チャット',
    },
    'musashikosugi': {
        '410972239': '【リベクリ】歯科武蔵小杉院｜日報・業務連絡チャット',
        '422210775': '【リベクリ】武蔵小杉院｜Drすり合わせチャット',
        '423373491': '【リベクリ通知】問合せ｜歯科武蔵小杉院',
        '424040510': '【リベクリ】武蔵小杉院｜発注チャット',
        '419643685': '【リベクリ｜歯科武蔵小杉院】シフトディレクション',
        '425799068': '【リベクリ】武蔵小杉院｜衛生士チームすり合わせチャット',
        '410085632': '【リベクリ】べてぃお先生・ABBさん・くまお_相談チャット',
        '412982781': '【リベクリ】松永先生xべてぃお先生xABBさんxくまおさん×Yuさん',
        '425576238': 'わっこ先生×べてぃお先生×マーラさん×くまおさん×ABB 連携チャット',
        '396511141': '【リベクリ】松永先生とやり取りするチャット',
    },
}

# 全ルームをフラットなdict（room_id -> {biz, name}）に展開
ALL_ROOMS = {}
for biz, rooms in ROOMS.items():
    for room_id, name in rooms.items():
        ALL_ROOMS[room_id] = {'biz': biz, 'name': name}


# ============================================================
# Chatwork API
# ============================================================

def cw_get(path, params=None):
    try:
        r = requests.get(f'{CW_BASE}{path}', headers=CW_HEADS,
                         params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        print(f'[WARN] Chatwork {r.status_code} {path}')
        return None
    except Exception as e:
        print(f'[ERROR] cw_get({path}): {e}')
        return None


def get_my_account():
    """自分のアカウント情報を取得"""
    data = cw_get('/me')
    if data:
        print(f'  自分のアカウント: {data.get("name")} (ID: {data.get("account_id")})')
    return data


def get_my_account2():
    """2つ目のアカウント情報を取得"""
    if not CHATWORK_TOKEN2:
        return None
    try:
        r = requests.get(f'{CW_BASE}/me',
                         headers={'X-ChatWorkToken': CHATWORK_TOKEN2}, timeout=30)
        if r.status_code == 200:
            data = r.json()
            print(f'  2つ目のアカウント: {data.get("name")} (ID: {data.get("account_id")})')
            return data
        print(f'[WARN] 2つ目のアカウント取得失敗: HTTP {r.status_code}')
    except Exception as e:
        print(f'[ERROR] get_my_account2: {e}')
    return None


def get_room_messages(room_id):
    """ルームのメッセージを取得（force=1で全件）"""
    data = cw_get(f'/rooms/{room_id}/messages', {'force': 1})
    return data if isinstance(data, list) else []


def filter_last_24h(messages, now_ts):
    """直近24時間のメッセージに絞り込む"""
    cutoff = now_ts - 86400  # 24時間前
    return [m for m in messages if m.get('send_time', 0) >= cutoff]


def fetch_all_messages(now_ts):
    """事業監視ルーム（22室）のメッセージを取得して事業別に整理"""
    print('事業ルームのChatworkメッセージ取得中...')
    biz_messages   = defaultdict(list)   # {biz: [{room_name, msg}, ...]}
    room_messages  = {}                  # {room_id: [msg, ...]}

    for room_id, meta in ALL_ROOMS.items():
        msgs = get_room_messages(room_id)
        recent = filter_last_24h(msgs, now_ts)
        room_messages[room_id] = recent
        for m in recent:
            biz_messages[meta['biz']].append({
                'room_id':   room_id,
                'room_name': meta['name'],
                'msg':       m,
            })
        print(f'  {meta["name"]}: {len(recent)}件（直近24h）')

    return biz_messages, room_messages


def fetch_all_my_room_messages(start_ts, end_ts, biz_room_messages):
    """Chatwork振り返り用メッセージ取得
    - 指定22ルーム（事業ルーム）: biz_room_messagesをそのまま利用
    - DM（direct）・マイチャット（my）: APIで追加取得
    グループチャット2400室を全取得しないよう、direct/myのみ追加する
    """
    print('DM・マイチャットのメッセージ取得中...')
    rooms_data = cw_get('/rooms')
    if not isinstance(rooms_data, list):
        print('[WARN] /rooms 取得失敗 → 事業ルームのみで振り返り')
        return dict(biz_room_messages), {}

    # direct / my タイプのルームのみ抽出（グループチャットはスキップ）
    dm_rooms = [r for r in rooms_data if r.get('type') in ('direct', 'my')]
    room_name_map = {str(r['room_id']): r.get('name', str(r['room_id'])) for r in rooms_data}
    print(f'  DM・マイチャット数: {len(dm_rooms)}件（全{len(rooms_data)}室中）')

    # 事業ルームのメッセージをベースにする（既取得・再利用）
    room_messages = {}
    for room_id, msgs in biz_room_messages.items():
        filtered = [m for m in msgs if start_ts <= m.get('send_time', 0) <= end_ts]
        if filtered:
            room_messages[room_id] = filtered

    # DM・マイチャットを追加取得（最近更新された順に上位80室まで）
    dm_rooms_sorted = sorted(dm_rooms, key=lambda r: r.get('last_update_time', 0), reverse=True)[:80]
    for room in dm_rooms_sorted:
        room_id = str(room.get('room_id', ''))
        if room_id in room_messages:
            continue
        msgs = get_room_messages(room_id)
        filtered = [m for m in msgs if start_ts <= m.get('send_time', 0) <= end_ts]
        if filtered:
            room_messages[room_id] = filtered
        time.sleep(0.7)  # レート制限対策（429エラー回避）

    total = sum(len(v) for v in room_messages.values())
    print(f'  対象日メッセージ合計: {total}件（{len(room_messages)}ルーム）')
    return room_messages, room_name_map


# ============================================================
# スプレッドシート取得
# ============================================================

def fetch_csv(sheet_id):
    """スプレッドシートをCSV文字列で取得"""
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv'
    try:
        r = requests.get(url, allow_redirects=True, timeout=30)
        if r.status_code == 200:
            r.encoding = 'utf-8'
            return r.text
        print(f'[WARN] Sheet HTTP {r.status_code} ({sheet_id})')
        return ''
    except Exception as e:
        print(f'[ERROR] fetch_csv({sheet_id}): {e}')
        return ''


def parse_transposed_monthly(csv_text, month):
    """
    横断型管理会計シート（行=項目, 列=月）をパースして当月KPIを返す
    引越センター・オンライン秘書共通
    当月データが未入力（revenue=0 かつ dealCount=0）の場合、前月にフォールバック
    """
    reader = csv.reader(io.StringIO(csv_text))
    rows   = list(reader)

    def to_int(v):
        try:
            return int(float(v))
        except:
            return 0

    def parse_col(col_idx):
        result = {}
        for row in rows:
            if len(row) <= col_idx:
                continue
            cat = row[0].strip()
            sub = row[1].strip() if len(row) > 1 else ''
            val = row[col_idx].strip().replace(',', '').replace('¥', '').replace('件', '').replace('%', '')
            key = f'{cat}_{sub}'
            result[key] = to_int(val)

        def find(patterns):
            for p in patterns:
                for k, v in result.items():
                    if all(x in k for x in p.split('|')):
                        return v
            return 0

        revenue    = find('売上|金額')
        deal_count = find('売上|件数')
        var_cost   = find('変動経費|合計')
        fix_cost   = find('固定経費|合計')
        profit     = find('利益') or find('最終利益')
        return revenue, deal_count, var_cost, fix_cost, profit

    # 当月を試みる
    col_idx = month + 1
    revenue, deal_count, var_cost, fix_cost, profit = parse_col(col_idx)

    # 当月データが未入力の場合、前月にフォールバック
    if revenue == 0 and deal_count == 0:
        prev_month = month - 1 if month > 1 else 12
        prev_col   = prev_month + 1
        revenue, deal_count, var_cost, fix_cost, profit = parse_col(prev_col)
        month_label = f'{prev_month}月実績（前月）'
        print(f'  [INFO] 当月({month}月)データ未入力 → {prev_month}月にフォールバック')
    else:
        month_label = f'{month}月累計'

    return {
        'revenue':    revenue,
        'dealCount':  deal_count,
        'varCost':    var_cost,
        'fixCost':    fix_cost,
        'profit':     profit,
        'monthLabel': month_label,
        'rawSummary': f'売上¥{revenue:,} / 件数{deal_count}件 / 変動費¥{var_cost:,} / 固定費¥{fix_cost:,} / 利益¥{profit:,}',
    }


def parse_daily_dental(csv_text, now):
    """武蔵小杉院の日次診療実績シートをパース"""
    reader = csv.reader(io.StringIO(csv_text))
    rows   = list(reader)
    year   = now.year
    reports = []

    for row in rows:
        if not row or not row[0]:
            continue
        date_str = row[0].strip()
        if not re.match(r'^\d{2}/\d{2}$', date_str):
            continue

        def col(i):
            if i >= len(row):
                return 0
            v = row[i].strip().replace(',', '')
            try:
                return int(float(v)) if v else 0
            except:
                return 0

        patients = col(2)
        total    = col(12)
        if patients == 0 and total == 0:
            continue

        try:
            m, d = date_str.split('/')
            dt   = datetime(year, int(m), int(d), 12, 0, 0, tzinfo=JST)
            ts   = int(dt.timestamp())
        except:
            ts = 0

        reports.append({
            'date':      date_str,
            'timestamp': ts,
            'insurance': {'count': col(4), 'amount': col(5)},
            'jihi':      {'count': col(6), 'amount': col(7)},
            'kyosei':    {'count': col(8), 'amount': col(9)},
            'hanpan':    {'count': col(10), 'amount': col(11)},
            'total':     total,
            'jissitsu':  patients,
            'jihiRate':  col(13),
        })

    reports.sort(key=lambda x: x['timestamp'])
    return reports


def fetch_member_list(sheet_id):
    """メンバーリストシートをテキストで取得（後で追加）"""
    if not sheet_id:
        return ''
    csv_text = fetch_csv(sheet_id)
    # 先頭30行を返す
    lines = csv_text.split('\n')[:30]
    return '\n'.join(lines)


# ============================================================
# Chatwork振り返り統計（Python側で計算）
# ============================================================

def calc_chatwork_stats(all_room_messages, my_account_ids, now, room_name_map=None):
    """自分が送ったメッセージの統計を計算（複数アカウント対応）"""
    room_name_map = room_name_map or {}
    # my_account_ids はセット（複数アカウントのIDをまとめて判定）
    if isinstance(my_account_ids, int):
        my_account_ids = {my_account_ids}
    else:
        my_account_ids = set(my_account_ids)

    def get_room_name(room_id):
        return (ALL_ROOMS.get(room_id, {}).get('name')
                or room_name_map.get(room_id)
                or room_id)

    my_msgs = []
    room_counts = defaultdict(int)   # {room_id: count}
    contact_counts = defaultdict(int) # {name: count}

    for room_id, msgs in all_room_messages.items():
        for m in msgs:
            acc = m.get('account', {})
            if acc.get('account_id') in my_account_ids:
                my_msgs.append({
                    'room_id':   room_id,
                    'room_name': get_room_name(room_id),
                    'body':      m.get('body', ''),
                    'send_time': m.get('send_time', 0),
                })
                room_counts[room_id] += 1
            else:
                # 自分以外のメッセージ送信者をカウント（やり取り相手）
                name = acc.get('name', '不明')
                contact_counts[name] += 1

    if not my_msgs:
        return None

    # 時系列ソート
    my_msgs.sort(key=lambda x: x['send_time'])
    times = [m['send_time'] for m in my_msgs]

    start_dt = datetime.fromtimestamp(times[0], tz=JST)
    end_dt   = datetime.fromtimestamp(times[-1], tz=JST)
    hours    = (times[-1] - times[0]) / 3600

    # ルーム別TOP10
    room_summary = []
    for room_id, cnt in sorted(room_counts.items(), key=lambda x: -x[1])[:10]:
        room_summary.append({
            'room_name': get_room_name(room_id),
            'count':     cnt,
        })

    # やり取り相手TOP10（自分がメッセージを送ったルームに存在する人）
    contacts_sorted = sorted(contact_counts.items(), key=lambda x: -x[1])[:10]

    # 曜日
    weekdays = ['月', '火', '水', '木', '金', '土', '日']
    date_label = start_dt.strftime(f'%-m/%-d（{weekdays[start_dt.weekday()]}）')

    return {
        'date':        date_label,
        'totalSent':   len(my_msgs),
        'startTime':   start_dt.strftime('%H:%M'),
        'endTime':     end_dt.strftime('%H:%M'),
        'activeHours': f'{hours:.1f}h',
        'roomCount':   len(room_counts),
        'roomSummary': room_summary,
        'contacts':    [{'name': n, 'count': c} for n, c in contacts_sorted],
        'myMessages':  my_msgs,  # Claude分析用（出力には含めない）
    }


# ============================================================
# メッセージフォーマット（Claude送信用）
# ============================================================

def format_biz_messages(biz_messages_list, max_per_room=30, max_chars=200):
    """事業別メッセージをClaude送信用テキストに整形"""
    lines = []
    # ルーム別にグループ化
    by_room = defaultdict(list)
    for item in biz_messages_list:
        by_room[item['room_name']].append(item['msg'])

    for room_name, msgs in by_room.items():
        lines.append(f'\n=== {room_name} ===')
        recent = msgs[-max_per_room:]
        for m in recent:
            dt   = datetime.fromtimestamp(m.get('send_time', 0), tz=JST)
            name = m.get('account', {}).get('name', '不明')
            body = m.get('body', '').strip()
            if body:
                body = body[:max_chars] + ('...' if len(body) > max_chars else '')
                lines.append(f'[{dt.strftime("%m/%d %H:%M")}] {name}: {body}')
    return '\n'.join(lines)


def format_my_messages(my_msgs, max_chars=200):
    """自分のメッセージをClaude送信用テキストに整形"""
    lines = []
    for m in my_msgs:
        dt   = datetime.fromtimestamp(m['send_time'], tz=JST)
        body = m['body'].strip()[:max_chars]
        lines.append(f'[{dt.strftime("%m/%d %H:%M")}] [{m["room_name"]}] {body}')
    return '\n'.join(lines)


# ============================================================
# Claude API 分析
# ============================================================

claude = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

def sanitize_json_text(text):
    """JSON文字列内の未エスケープ改行・制御文字を修正する"""
    result = []
    in_string = False
    escape_next = False
    for ch in text:
        if escape_next:
            result.append(ch)
            escape_next = False
        elif ch == '\\':
            result.append(ch)
            escape_next = True
        elif ch == '"':
            in_string = not in_string
            result.append(ch)
        elif in_string and ch == '\n':
            result.append('\\n')
        elif in_string and ch == '\r':
            result.append('\\r')
        elif in_string and ch == '\t':
            result.append('\\t')
        else:
            result.append(ch)
    return ''.join(result)


def fix_missing_commas(text):
    """JSON構造の欠落カンマを行単位で修正する（sanitize_json_text後に呼ぶ）"""
    lines = text.split('\n')
    fixed = []
    for i, line in enumerate(lines[:-1]):
        stripped = line.rstrip()
        next_stripped = lines[i + 1].lstrip()
        ends_with_value = (
            stripped.endswith('"') or
            stripped.endswith('}') or
            stripped.endswith(']') or
            (stripped and stripped[-1].isdigit()) or
            stripped.endswith('true') or
            stripped.endswith('false') or
            stripped.endswith('null')
        )
        next_starts_new_element = (
            next_stripped.startswith('"') or
            next_stripped.startswith('{') or
            next_stripped.startswith('[')
        )
        if ends_with_value and next_starts_new_element:
            fixed.append(stripped + ',')
        else:
            fixed.append(line)
    fixed.append(lines[-1])
    return '\n'.join(fixed)


def call_claude(prompt, max_tokens=4096, label=''):
    """Claude APIを呼び出してJSONを返す（最大3回リトライ）"""
    for attempt in range(3):
        try:
            msg = claude.messages.create(
                model='claude-sonnet-4-6',
                max_tokens=max_tokens,
                messages=[{'role': 'user', 'content': prompt}]
            )
            text = msg.content[0].text.strip()
            s = text.find('{')
            e = text.rfind('}')
            if s != -1 and e > s:
                json_str = fix_missing_commas(sanitize_json_text(text[s:e+1]))
                return json.loads(json_str)
            print(f'[WARN] Claude ({label}): JSONが見つかりません')
            return {}
        except json.JSONDecodeError as je:
            print(f'[WARN] Claude ({label}): JSON解析エラー: {je}')
            print(f'  レスポンス先頭200字: {text[:200]}')
            return {}
        except Exception as ex:
            print(f'[ERROR] Claude ({label}): {ex}')
            if attempt < 2:
                print(f'  60秒後にリトライ ({attempt+1}/3)...')
                time.sleep(60)
    return {}


def analyze_risks(all_biz_context, revenue_context, today_str):
    """リスクアラート + ポジティブシグナル + ヘッダーアラートを分析"""
    prompt = f"""あなたは経営コンサルタントです。
以下は「{today_str}」の3事業（リベ大デンタルクリニック武蔵小杉院・リベ大引越センター・リベ大オンライン秘書）の直近24時間のChatworkログと収益データです。

{all_biz_context}

収益データ:
{revenue_context}

---
経営者が毎朝確認するリスクレポートとして、以下のJSON形式で返してください。日本語で具体的に記述し、数値・人名・出来事を積極的に使ってください。

{{
  "alertHeadline": "高リスクN件 / 最重要イベント / ...",
  "alertSub": "ポジティブな1行サマリー（例：〇〇院売上好調・〇〇採用完了）",
  "risks": {{
    "high": [
      {{"title": "[高] 事業名 リスクタイトル", "body": "状況の詳細（200字程度）", "action": "→ 推奨アクション（誰が・何を・いつまでに）"}}
    ],
    "medium": [
      {{"category": "カテゴリ（クレーム/コンプライアンス/先送り等）", "summary": "概要（80字程度）", "action": "推奨対応", "deadline": "期限（早急に/今週中/週明け等）"}}
    ],
    "low": [
      {{"case": "案件名", "summary": "概要（60字程度）", "action": "推奨対応"}}
    ]
  }},
  "signals": [
    {{"name": "事業名", "text": "ポジティブな内容（具体的な数値や出来事を含む）"}}
  ]
}}

JSONのみを返してください。"""

    return call_claude(prompt, max_tokens=6000, label='risks')


def analyze_chatwork_review(my_msgs_text, stats, today_str):
    """Chatwork振り返り（自分のメッセージ）を分析"""
    prompt = f"""あなたは経営アシスタントです。
以下は経営者（あなたの依頼主）が{today_str}に送信したChatworkメッセージ一覧です。

{my_msgs_text}

統計情報:
- 総送信数: {stats['totalSent']}件
- 活動開始: {stats['startTime']}
- 活動終了: {stats['endTime']}
- 活動時間: {stats['activeHours']}
- ルーム数: {stats['roomCount']}ルーム

ルーム別送信数:
{chr(10).join([f"  {i+1}. {r['room_name']}: {r['count']}件" for i, r in enumerate(stats['roomSummary'])])}

---
以下のJSON形式で、経営者自身のコミュニケーション活動を振り返るレポートを作成してください。

{{
  "roomSummary": [
    {{"rank": 1, "room": "ルーム名", "count": 件数, "topics": "主なトピック（50字程度）"}}
  ],
  "contacts": [
    {{"rank": 1, "name": "やり取りした相手名", "summary": "主なやり取り内容"}}
  ],
  "done": ["完了した事項1（具体的に）", "完了した事項2"],
  "progress": ["進展した事項や新規課題1", "進展した事項や新規課題2"],
  "decisions": [
    {{"content": "意思決定の内容（〇〇を決定: 詳細）"}}
  ],
  "tone": "感情・トーン分析（150字程度）",
  "improve": "改善ポイント（100字程度）",
  "carryover": [
    {{"content": "翌日持ち越し事項", "status": "top/ongoing/watch"}}
  ]
}}

JSONのみを返してください。"""

    return call_claude(prompt, max_tokens=6000, label='chatwork')


def analyze_business(biz_key, biz_name, context, revenue_data, member_context, today_str):
    """各事業の深掘り分析"""
    member_section = f"\nメンバーリスト:\n{member_context}" if member_context else ''

    prompt = f"""あなたは{biz_name}の経営コンサルタントです。
以下は{today_str}の直近24時間のChatworkログ{('と' + revenue_data['rawSummary']) if revenue_data else ''}です。
{member_section}

Chatworkログ:
{context}

---
以下のJSON形式で、経営者向けの深掘り分析レポートを作成してください。
chatworkログに登場する人物からスタッフ構成を分析し、各スタッフの状況を把握してください。

{{
  "bannerSub": "1行ステータスサマリー（ABB常駐・〇〇進行中・本日のポイント等）",
  "metrics": [
    {{"val": "数値", "lbl": "指標名", "sub": "補足", "cls": "（赤字なら red, 好調なら green, それ以外は空文字）"}}
  ],
  "staff": [
    {{"name": "スタッフ名", "role": "役割", "note": "直近の状況・備考（chatworkログから読み取る）", "status": "normal/warning/good"}}
  ],
  "issues": [
    {{"title": "課題タイトル", "body": "詳細（120字程度）", "action": "→ 推奨アクション"}}
  ],
  "actions": [
    {{"period": "今週", "items": ["アクション1", "アクション2"]}},
    {{"period": "来月以降", "items": ["中期アクション1"]}}
  ]
}}

JSONのみを返してください。"""

    return call_claude(prompt, max_tokens=8000, label=biz_key)


def analyze_biz_report(all_summaries, today_str):
    """事業全体サマリーを分析"""
    prompt = f"""あなたは経営コンサルタントです。
以下は{today_str}の3事業の状況サマリーです。

{all_summaries}

---
以下のJSON形式で、全体サマリーを作成してください。

{{
  "good": "好調な事業とその理由（具体的な数値を含む、150字程度）",
  "caution": "注意が必要な事業とその理由（具体的に、100字程度）"
}}

JSONのみを返してください。"""

    return call_claude(prompt, max_tokens=1000, label='bizreport')


# ============================================================
# メイン
# ============================================================

def main():
    now     = datetime.now(JST)
    now_ts  = int(now.timestamp())
    today_str = now.strftime('%Y年%m月%d日')
    month   = now.month

    # Chatwork振り返り対象日 = 前日（0:00〜23:59 JST）
    yesterday       = (now - timedelta(days=1)).date()
    cw_start_ts     = int(datetime(yesterday.year, yesterday.month, yesterday.day,  0,  0,  0, tzinfo=JST).timestamp())
    cw_end_ts       = int(datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, tzinfo=JST).timestamp())

    print(f'=== Morning Dashboard 生成開始: {now.strftime("%Y-%m-%d %H:%M JST")} ===')

    # ─── 1. 自分のアカウント情報取得 ───
    print('\n[1] 自分のChatworkアカウント取得中...')
    me = get_my_account()
    my_account_id = me.get('account_id') if me else None
    if not my_account_id:
        print('[WARN] アカウントIDを取得できませんでした')

    me2 = get_my_account2()
    my_account_id2 = me2.get('account_id') if me2 else None

    # 2アカウントをセットにまとめる
    my_account_ids = {i for i in [my_account_id, my_account_id2] if i}

    # ─── 2. Chatworkメッセージ取得 ───
    print('\n[2] Chatworkメッセージ取得中...')
    biz_messages, room_messages = fetch_all_messages(now_ts)

    # ─── 3. スプレッドシート取得 ───
    print('\n[3] スプレッドシート取得中...')
    revenue = {}

    print('  引越センター...')
    hikkoshi_csv = fetch_csv(SHEETS['hikkoshi'])
    revenue['hikkoshi'] = parse_transposed_monthly(hikkoshi_csv, month)

    print('  オンライン秘書...')
    hisho_csv = fetch_csv(SHEETS['hisho'])
    revenue['hisho'] = parse_transposed_monthly(hisho_csv, month)

    print('  武蔵小杉院...')
    musashi_csv = fetch_csv(SHEETS['musashikosugi'])
    musashi_reports = parse_daily_dental(musashi_csv, now)
    # 武蔵小杉院の月次集計
    m_total     = sum(r['total']              for r in musashi_reports)
    m_patients  = sum(r['jissitsu']           for r in musashi_reports)
    m_jihi      = sum(r['jihi']['amount']     for r in musashi_reports)
    m_insurance = sum(r['insurance']['amount'] for r in musashi_reports)
    m_workdays  = len(musashi_reports)
    jihi_rate   = round(m_jihi / m_total * 100) if m_total > 0 else 0
    latest      = musashi_reports[-1] if musashi_reports else None
    revenue['musashikosugi'] = {
        'revenue':    m_total,
        'dealCount':  m_patients,
        'varCost':    0,
        'fixCost':    0,
        'profit':     0,
        'rawSummary': f'月間累計¥{m_total:,} / {m_workdays}診療日 / 平均{round(m_patients/m_workdays) if m_workdays else 0}人/日 / 自費率{jihi_rate}%',
        'dailyReports': musashi_reports,
        'latest':     latest,
    }
    print(f'  武蔵小杉院: {m_workdays}日分 / 月累計¥{m_total:,}')

    # ─── 4. メンバーリスト取得（URLが設定されている場合） ───
    print('\n[4] メンバーリスト取得中...')
    member_contexts = {}
    for biz in ['hikkoshi', 'hisho', 'musashikosugi']:
        member_contexts[biz] = fetch_member_list(MEMBER_SHEETS[biz])

    # ─── 5. Chatwork振り返り統計計算（全ルーム・DM含む） ───
    print('\n[5] Chatwork振り返り計算中（全ルーム・DM含む）...')
    all_room_msgs, room_name_map = fetch_all_my_room_messages(cw_start_ts, cw_end_ts, room_messages)
    cw_stats = None
    if my_account_id:
        cw_stats = calc_chatwork_stats(all_room_msgs, my_account_ids, now, room_name_map)
        if cw_stats:
            print(f'  自分の送信: {cw_stats["totalSent"]}件 / {cw_stats["startTime"]}〜{cw_stats["endTime"]}')

    # ─── 6. Claude分析 ───
    print('\n[6] Claude API 分析中...')

    # 全事業コンテキスト（リスク分析用）
    all_biz_context = ''
    for biz, biz_name in [('hikkoshi','引越センター'), ('hisho','オンライン秘書'), ('musashikosugi','武蔵小杉院')]:
        all_biz_context += f'\n\n【{biz_name}】\n'
        all_biz_context += format_biz_messages(biz_messages[biz])

    revenue_context = '\n'.join([
        f'引越センター: {revenue["hikkoshi"]["rawSummary"]}',
        f'オンライン秘書: {revenue["hisho"]["rawSummary"]}',
        f'武蔵小杉院: {revenue["musashikosugi"]["rawSummary"]}',
    ])

    # 6-1. リスク分析
    print('  6-1. リスク分析...')
    risk_result = analyze_risks(all_biz_context, revenue_context, today_str)

    # 6-2. Chatwork振り返り
    cw_analysis = {}
    if cw_stats and cw_stats.get('myMessages'):
        print('  6-2. Chatwork振り返り分析...')
        my_msgs_text = format_my_messages(cw_stats['myMessages'])
        cw_analysis  = analyze_chatwork_review(my_msgs_text, cw_stats, today_str)

    # 6-3. 各事業深掘り
    biz_details = {}
    biz_summaries = ''
    for biz, biz_name in [('hikkoshi','リベ大引越センター'), ('hisho','リベ大オンライン秘書'), ('musashikosugi','リベ大デンタルクリニック武蔵小杉院')]:
        print(f'  6-3. {biz_name} 深掘り分析...')
        ctx    = format_biz_messages(biz_messages[biz])
        detail = analyze_business(biz, biz_name, ctx, revenue[biz], member_contexts[biz], today_str)
        biz_details[biz] = detail
        biz_summaries += f'\n【{biz_name}】\n{detail.get("bannerSub","")}\n'
        time.sleep(2)  # レート制限対策

    # 6-4. 全体サマリー
    print('  6-4. 全体サマリー...')
    biz_report = analyze_biz_report(biz_summaries, today_str)

    # ─── 7. data.json 組み立て ───
    print('\n[7] data.json 組み立て中...')

    def safe(d, key, default=''):
        return d.get(key, default) if d else default

    # Chatwork振り返りデータ統合
    chatwork_data = {}
    if cw_stats:
        chatwork_data = {
            'date':        cw_stats.get('date', ''),
            'totalSent':   cw_stats.get('totalSent', 0),
            'startTime':   cw_stats.get('startTime', '—'),
            'endTime':     cw_stats.get('endTime', '—'),
            'activeHours': cw_stats.get('activeHours', '—'),
            'roomCount':   cw_stats.get('roomCount', 0),
            'roomSummary': [
                {
                    'rank':   i + 1,
                    'room':   r.get('room_name', ''),
                    'count':  r.get('count', 0),
                    'topics': '',  # Claude分析で補完
                }
                for i, r in enumerate(cw_stats.get('roomSummary', []))
            ],
            'contacts':  [],
            'done':      [],
            'progress':  [],
            'decisions': [],
            'tone':      '',
            'improve':   '',
            'carryover': [],
        }
        # Claude分析結果をマージ
        if cw_analysis:
            for i, room in enumerate(chatwork_data['roomSummary']):
                for ca in cw_analysis.get('roomSummary', []):
                    if ca.get('rank') == i + 1:
                        room['topics'] = ca.get('topics', '')
            chatwork_data['contacts']  = cw_analysis.get('contacts', [])
            chatwork_data['done']      = cw_analysis.get('done', [])
            chatwork_data['progress']  = cw_analysis.get('progress', [])
            chatwork_data['decisions'] = cw_analysis.get('decisions', [])
            chatwork_data['tone']      = cw_analysis.get('tone', '')
            chatwork_data['improve']   = cw_analysis.get('improve', '')
            chatwork_data['carryover'] = cw_analysis.get('carryover', [])

    # 武蔵小杉院メトリクスを数値から生成
    musashi_metrics_default = []
    if latest:
        musashi_metrics_default = [
            {'val': f'¥{latest["total"]:,}',        'lbl': '直近売上',   'sub': latest['date'] + ' 日次', 'cls': ''},
            {'val': f'{latest["jissitsu"]}人',        'lbl': '患者数',     'sub': latest['date'],            'cls': ''},
            {'val': f'¥{m_total:,}',                 'lbl': '月間累計',   'sub': f'{m_workdays}診療日',     'cls': ''},
            {'val': f'{jihi_rate}%',                  'lbl': '自費率',     'sub': '',                        'cls': ''},
        ]

    # 引越センター・オンライン秘書のデフォルトメトリクス
    def make_metrics(rev):
        month_label = rev.get('monthLabel', f'{now.month}月累計')
        return [
            {'val': f'¥{rev["revenue"]:,}',   'lbl': '今月売上',   'sub': month_label,          'cls': 'green' if rev["revenue"] > 0 else ''},
            {'val': f'{rev["dealCount"]}件',   'lbl': '売上件数',   'sub': '',                   'cls': ''},
            {'val': f'¥{rev["varCost"]:,}',   'lbl': '変動経費',   'sub': '',                   'cls': ''},
            {'val': f'¥{rev["profit"]:,}',    'lbl': '利益',       'sub': '',                   'cls': 'green' if rev["profit"] > 0 else 'red'},
        ]

    data = {
        'date':         now.strftime('%Y-%m-%d'),
        'generatedAt':  now.strftime('%H:%M'),
        'updatedAtLabel': now.strftime('%Y年%m月%d日 %H:%M'),

        'alerts': {
            'headline': safe(risk_result, 'alertHeadline', '本日のアラート情報はありません'),
            'sub':      safe(risk_result, 'alertSub', ''),
        },

        'risks': {
            'high':    safe(risk_result, 'risks', {}).get('high', []),
            'medium':  safe(risk_result, 'risks', {}).get('medium', []),
            'low':     safe(risk_result, 'risks', {}).get('low', []),
            'signals': safe(risk_result, 'signals', []),
        },

        'chatworkReview': chatwork_data,

        'bizReport': {
            'good':    safe(biz_report, 'good', ''),
            'caution': safe(biz_report, 'caution', ''),
        },

        'musashikosugi': {
            'bannerSub': safe(biz_details.get('musashikosugi'), 'bannerSub', ''),
            'metrics':   biz_details.get('musashikosugi', {}).get('metrics', musashi_metrics_default),
            'staff':     safe(biz_details.get('musashikosugi'), 'staff', []),
            'issues':    safe(biz_details.get('musashikosugi'), 'issues', []),
            'actions':   safe(biz_details.get('musashikosugi'), 'actions', []),
            'dailyReports': musashi_reports,
        },

        'hikkoshi': {
            'bannerSub': safe(biz_details.get('hikkoshi'), 'bannerSub', ''),
            'metrics':   biz_details.get('hikkoshi', {}).get('metrics', make_metrics(revenue['hikkoshi'])),
            'staff':     safe(biz_details.get('hikkoshi'), 'staff', []),
            'issues':    safe(biz_details.get('hikkoshi'), 'issues', []),
            'actions':   safe(biz_details.get('hikkoshi'), 'actions', []),
            'revenue':   revenue['hikkoshi'],
        },

        'hisho': {
            'bannerSub': safe(biz_details.get('hisho'), 'bannerSub', ''),
            'metrics':   biz_details.get('hisho', {}).get('metrics', make_metrics(revenue['hisho'])),
            'staff':     safe(biz_details.get('hisho'), 'staff', []),
            'issues':    safe(biz_details.get('hisho'), 'issues', []),
            'actions':   safe(biz_details.get('hisho'), 'actions', []),
            'revenue':   revenue['hisho'],
        },
    }

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f'\n=== 完了: {now.strftime("%Y-%m-%d %H:%M JST")} ===')
    print(f'  自分の送信: {chatwork_data.get("totalSent", 0)}件')
    print(f'  リスク(高/中/低): {len(data["risks"]["high"])}/{len(data["risks"]["medium"])}/{len(data["risks"]["low"])}')
    print(f'  武蔵小杉院 月累計: ¥{m_total:,}')
    print(f'  引越センター 月売上: ¥{revenue["hikkoshi"]["revenue"]:,}')
    print(f'  オンライン秘書 月売上: ¥{revenue["hisho"]["revenue"]:,}')


if __name__ == '__main__':
    main()
