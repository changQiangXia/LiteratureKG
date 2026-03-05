from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import http.client
import json
import random
import re
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from opencc import OpenCC
except Exception:  # pragma: no cover - optional dependency
    OpenCC = None


API_ROOT = "https://api.github.com/repos/chinese-poetry/chinese-poetry"
CODELOAD_ZIP_URL = "https://codeload.github.com/chinese-poetry/chinese-poetry/zip/refs/heads/master"
USER_AGENT = "LiteratureKG-MVP/1.0"

_OPENCC = OpenCC("t2s") if OpenCC is not None else None
_FALLBACK_T2S = str.maketrans(
    {
        "萬": "万",
        "與": "与",
        "為": "为",
        "國": "国",
        "學": "学",
        "體": "体",
        "關": "关",
        "風": "风",
        "雲": "云",
        "驛": "驿",
        "樓": "楼",
        "橋": "桥",
        "臺": "台",
        "閣": "阁",
        "陽": "阳",
        "陰": "阴",
        "廣": "广",
        "蘇": "苏",
        "臨": "临",
        "嶺": "岭",
        "漢": "汉",
        "劍": "剑",
        "濟": "济",
        "鄉": "乡",
        "愛": "爱",
        "憶": "忆",
        "懷": "怀",
        "歸": "归",
        "邊": "边",
        "華": "华",
        "東": "东",
        "西": "西",
        "南": "南",
        "北": "北",
        "長": "长",
        "門": "门",
        "開": "开",
        "闕": "阙",
        "濤": "涛",
        "瀟": "潇",
        "澤": "泽",
        "滄": "沧",
        "漁": "渔",
        "燈": "灯",
        "鐘": "钟",
        "鶴": "鹤",
        "鷗": "鸥",
    }
)


def to_simplified(text: str) -> str:
    if not text:
        return text
    if _OPENCC is not None:
        return _OPENCC.convert(text)
    return text.translate(_FALLBACK_T2S)


PLACE_CITY_MAP: dict[str, tuple[str, str]] = {
    "长安": ("c_changan", "长安"),
    "長安": ("c_changan", "长安"),
    "洛阳": ("c_luoyang", "洛阳"),
    "洛陽": ("c_luoyang", "洛阳"),
    "汴京": ("c_bianjing", "汴京"),
    "汴梁": ("c_bianjing", "汴京"),
    "开封": ("c_bianjing", "汴京"),
    "開封": ("c_bianjing", "汴京"),
    "临安": ("c_linan", "临安"),
    "臨安": ("c_linan", "临安"),
    "杭州": ("c_linan", "临安"),
    "錢塘": ("c_linan", "临安"),
    "钱塘": ("c_linan", "临安"),
    "金陵": ("c_jinling", "金陵"),
    "建康": ("c_jinling", "金陵"),
    "江宁": ("c_jinling", "金陵"),
    "江寧": ("c_jinling", "金陵"),
    "姑苏": ("c_suzhou", "姑苏"),
    "姑蘇": ("c_suzhou", "姑苏"),
    "苏州": ("c_suzhou", "姑苏"),
    "蘇州": ("c_suzhou", "姑苏"),
    "扬州": ("c_yangzhou", "扬州"),
    "揚州": ("c_yangzhou", "扬州"),
    "成都": ("c_chengdu", "成都"),
    "蜀中": ("c_chengdu", "成都"),
    "夔州": ("c_kuizhou", "夔州"),
    "岳阳": ("c_yueyang", "岳阳"),
    "岳陽": ("c_yueyang", "岳阳"),
    "洞庭": ("c_yueyang", "岳阳"),
    "巴陵": ("c_yueyang", "岳阳"),
    "庐山": ("c_lushan", "庐山"),
    "廬山": ("c_lushan", "庐山"),
    "终南": ("c_zhongnan", "终南"),
    "終南": ("c_zhongnan", "终南"),
    "西湖": ("c_xihu", "西湖"),
    "太湖": ("c_taihu", "太湖"),
    "黄河": ("c_huanghe", "黄河"),
    "黃河": ("c_huanghe", "黄河"),
    "渭水": ("c_weishui", "渭水"),
    "淮水": ("c_huaishui", "淮水"),
    "秦淮": ("c_huaishui", "淮水"),
    "湘江": ("c_xiangjiang", "湘江"),
    "汉江": ("c_hanjiang", "汉江"),
    "漢江": ("c_hanjiang", "汉江"),
    "吴江": ("c_wujiang", "吴江"),
    "吳江": ("c_wujiang", "吴江"),
    "灞桥": ("c_changan", "长安"),
    "灞橋": ("c_changan", "长安"),
    "朱雀桥": ("c_changan", "长安"),
    "朱雀橋": ("c_changan", "长安"),
    "曲江": ("c_changan", "长安"),
    "浔阳": ("c_xunyang", "浔阳"),
    "潯陽": ("c_xunyang", "浔阳"),
    "浔阳江": ("c_xunyang", "浔阳"),
    "潯陽江": ("c_xunyang", "浔阳"),
    "姑孰": ("c_xuancheng", "宣城"),
    "宣城": ("c_xuancheng", "宣城"),
    "武昌": ("c_wuchang", "武昌"),
    "黄州": ("c_huangzhou", "黄州"),
    "黃州": ("c_huangzhou", "黄州"),
    "赤壁": ("c_huangzhou", "黄州"),
    "滕王阁": ("c_nanchang", "南昌"),
    "滕王閣": ("c_nanchang", "南昌"),
    "豫章": ("c_nanchang", "南昌"),
    "长江": ("c_changjiang", "长江"),
    "長江": ("c_changjiang", "长江"),
    "黄鹤楼": ("c_wuchang", "武昌"),
    "黃鶴樓": ("c_wuchang", "武昌"),
}

# Extra place aliases to increase recall for Tang/Song poetry.
PLACE_CITY_MAP.update(
    {
        "江南": ("c_jiangnan", "江南"),
        "江北": ("c_jiangbei", "江北"),
        "河北": ("c_hebei", "河北"),
        "河南": ("c_henan", "河南"),
        "关中": ("c_guanzhong", "关中"),
        "關中": ("c_guanzhong", "关中"),
        "关东": ("c_guandong", "关东"),
        "關東": ("c_guandong", "关东"),
        "中原": ("c_zhongyuan", "中原"),
        "塞北": ("c_saibei", "塞北"),
        "塞外": ("c_saiwai", "塞外"),
        "西域": ("c_xiyu", "西域"),
        "巴蜀": ("c_bashu", "巴蜀"),
        "吴越": ("c_wuyue", "吴越"),
        "吳越": ("c_wuyue", "吴越"),
        "岭南": ("c_lingnan", "岭南"),
        "嶺南": ("c_lingnan", "岭南"),
        "荆楚": ("c_jingchu", "荆楚"),
        "荊楚": ("c_jingchu", "荆楚"),
        "燕赵": ("c_yanzhao", "燕赵"),
        "燕趙": ("c_yanzhao", "燕赵"),
        "三秦": ("c_sanqin", "三秦"),
        "京口": ("c_jingkou", "京口"),
        "广陵": ("c_yangzhou", "扬州"),
        "廣陵": ("c_yangzhou", "扬州"),
        "润州": ("c_runzhou", "润州"),
        "潤州": ("c_runzhou", "润州"),
        "瓜洲": ("c_guazhou", "瓜洲"),
        "江陵": ("c_jiangling", "江陵"),
        "荆州": ("c_jiangling", "江陵"),
        "荊州": ("c_jiangling", "江陵"),
        "襄阳": ("c_xiangyang", "襄阳"),
        "襄陽": ("c_xiangyang", "襄阳"),
        "樊城": ("c_xiangyang", "襄阳"),
        "夔门": ("c_kuizhou", "夔州"),
        "夔門": ("c_kuizhou", "夔州"),
        "夔峡": ("c_kuizhou", "夔州"),
        "夔峽": ("c_kuizhou", "夔州"),
        "并州": ("c_bingzhou", "并州"),
        "并州": ("c_bingzhou", "并州"),
        "幽州": ("c_youzhou", "幽州"),
        "燕京": ("c_yanjing", "燕京"),
        "渝州": ("c_yuzhou", "渝州"),
        "梓州": ("c_zizhou", "梓州"),
        "益州": ("c_yizhou", "益州"),
        "蜀州": ("c_shuzhou", "蜀州"),
        "鄂州": ("c_ezhou", "鄂州"),
        "洪州": ("c_hongzhou", "洪州"),
        "豫章": ("c_nanchang", "南昌"),
        "彭泽": ("c_pengze", "彭泽"),
        "彭澤": ("c_pengze", "彭泽"),
        "鄱阳": ("c_poyang", "鄱阳"),
        "鄱陽": ("c_poyang", "鄱阳"),
        "庐州": ("c_luzhou", "庐州"),
        "廬州": ("c_luzhou", "庐州"),
        "汝州": ("c_ruzhou", "汝州"),
        "汴州": ("c_bianzhou", "汴州"),
        "梁园": ("c_liangyuan", "梁园"),
        "梁園": ("c_liangyuan", "梁园"),
        "邺城": ("c_yecheng", "邺城"),
        "鄴城": ("c_yecheng", "邺城"),
        "建业": ("c_jianye", "建业"),
        "建業": ("c_jianye", "建业"),
        "京城": ("c_jingcheng", "京城"),
        "两京": ("c_liangjing", "两京"),
        "兩京": ("c_liangjing", "两京"),
        "函谷关": ("c_hangu", "函谷关"),
        "函谷關": ("c_hangu", "函谷关"),
        "玉门关": ("c_yumenguan", "玉门关"),
        "玉門關": ("c_yumenguan", "玉门关"),
        "阳关": ("c_yangguan", "阳关"),
        "陽關": ("c_yangguan", "阳关"),
        "潼关": ("c_tongguan", "潼关"),
        "潼關": ("c_tongguan", "潼关"),
        "蓝田": ("c_lantian", "蓝田"),
        "藍田": ("c_lantian", "蓝田"),
        "终南山": ("c_zhongnan", "终南"),
        "終南山": ("c_zhongnan", "终南"),
        "华山": ("c_huashan", "华山"),
        "華山": ("c_huashan", "华山"),
        "泰山": ("c_taishan", "泰山"),
        "嵩山": ("c_songshan", "嵩山"),
        "衡山": ("c_hengshan", "衡山"),
        "岱宗": ("c_taishan", "泰山"),
        "巫山": ("c_wushan", "巫山"),
        "巴山": ("c_bashan", "巴山"),
        "匡庐": ("c_lushan", "庐山"),
        "匡廬": ("c_lushan", "庐山"),
        "洞庭湖": ("c_yueyang", "洞庭"),
        "镜湖": ("c_jinghu", "镜湖"),
        "鏡湖": ("c_jinghu", "镜湖"),
        "钱塘江": ("c_linan", "钱塘"),
        "錢塘江": ("c_linan", "钱塘"),
        "湘水": ("c_xiangjiang", "湘江"),
        "沅江": ("c_yuanjiang", "沅江"),
        "赣江": ("c_ganjiang", "赣江"),
        "贛江": ("c_ganjiang", "赣江"),
        "闽江": ("c_minjiang", "闽江"),
        "閩江": ("c_minjiang", "闽江"),
        "淮河": ("c_huaishui", "淮河"),
        "渭河": ("c_weishui", "渭河"),
        "汉水": ("c_hanjiang", "汉江"),
        "漢水": ("c_hanjiang", "汉江"),
        "长安城": ("c_changan", "长安"),
        "長安城": ("c_changan", "长安"),
        "洛阳城": ("c_luoyang", "洛阳"),
        "洛陽城": ("c_luoyang", "洛阳"),
        "临安城": ("c_linan", "临安"),
        "臨安城": ("c_linan", "临安"),
        "汴水": ("c_bianshui", "汴水"),
        "渭城": ("c_weicheng", "渭城"),
        "曲江池": ("c_changan", "曲江"),
        "灞陵": ("c_changan", "灞陵"),
        "吴门": ("c_suzhou", "姑苏"),
        "吳門": ("c_suzhou", "姑苏"),
        "姑苏台": ("c_suzhou", "姑苏"),
        "姑蘇臺": ("c_suzhou", "姑苏"),
        "姑苏城": ("c_suzhou", "姑苏"),
        "姑蘇城": ("c_suzhou", "姑苏"),
        "金陵城": ("c_jinling", "金陵"),
        "石头城": ("c_jinling", "金陵"),
        "石頭城": ("c_jinling", "金陵"),
        "采石矶": ("c_caishiji", "采石矶"),
        "采石磯": ("c_caishiji", "采石矶"),
        "赤壁矶": ("c_huangzhou", "赤壁"),
        "赤壁磯": ("c_huangzhou", "赤壁"),
        "岳阳楼": ("c_yueyang", "岳阳"),
        "岳陽樓": ("c_yueyang", "岳阳"),
        "滕王阁": ("c_nanchang", "南昌"),
        "滕王閣": ("c_nanchang", "南昌"),
        "鹳雀楼": ("c_guanquelou", "鹳雀楼"),
        "鸛雀樓": ("c_guanquelou", "鹳雀楼"),
        "白帝城": ("c_kuizhou", "夔州"),
        "长安道": ("c_changan", "长安"),
        "長安道": ("c_changan", "长安"),
        "洛阳道": ("c_luoyang", "洛阳"),
        "洛陽道": ("c_luoyang", "洛阳"),
        "蜀道": ("c_shudao", "蜀道"),
        "秦岭": ("c_qinling", "秦岭"),
        "秦嶺": ("c_qinling", "秦岭"),
        "太行": ("c_taihang", "太行"),
        "天山": ("c_tianshan", "天山"),
        "祁连": ("c_qilian", "祁连"),
        "祁連": ("c_qilian", "祁连"),
        "岷山": ("c_minshan", "岷山"),
        "蜀江": ("c_shujiang", "蜀江"),
        "兰亭": ("c_lanting", "兰亭"),
        "蘭亭": ("c_lanting", "兰亭"),
        "会稽": ("c_kuaiji", "会稽"),
        "會稽": ("c_kuaiji", "会稽"),
        "越州": ("c_yuezhou", "越州"),
        "杭州": ("c_linan", "临安"),
        "苏州": ("c_suzhou", "姑苏"),
        "蘇州": ("c_suzhou", "姑苏"),
        "扬州": ("c_yangzhou", "扬州"),
        "揚州": ("c_yangzhou", "扬州"),
        "青州": ("c_qingzhou", "青州"),
        "齐州": ("c_qizhou", "齐州"),
        "齊州": ("c_qizhou", "齐州"),
        "徐州": ("c_xuzhou", "徐州"),
        "泗州": ("c_sizhou", "泗州"),
        "福州": ("c_fuzhou", "福州"),
        "泉州": ("c_quanzhou", "泉州"),
        "温州": ("c_wenzhou", "温州"),
        "明州": ("c_mingzhou", "明州"),
        "台州": ("c_taizhou", "台州"),
        "臺州": ("c_taizhou", "台州"),
    }
)

GENERIC_REGION_TERMS = [
    "江南",
    "江北",
    "河北",
    "河南",
    "关中",
    "关东",
    "中原",
    "塞北",
    "塞外",
    "西域",
    "巴蜀",
    "吴越",
    "岭南",
    "荆楚",
    "燕赵",
    "三秦",
]

PLACE_PATTERN_CORE = re.compile(
    r"[\u4e00-\u9fff]{1,2}(?:州|郡|县|府|城|京|江|河|湖|海|溪|山|岭|关|門|门|津|浦|洲|渡)"
)
PLACE_PATTERN_BUILDING = re.compile(
    r"[\u4e00-\u9fff]{2,3}(?:楼|樓|阁|閣|桥|橋|亭|台|臺|寺|宫|宮)"
)
PLACE_PATTERN_WATER = re.compile(r"[\u4e00-\u9fff]{1,2}水")

PLACE_STOPWORDS = {
    "青山",
    "白山",
    "高山",
    "山川",
    "江湖",
    "江山",
    "故乡",
    "故鄉",
    "故园",
    "故園",
    "他乡",
    "他鄉",
    "高楼",
    "孤城",
    "空城",
    "旧城",
    "舊城",
    "春江",
    "秋江",
    "寒江",
    "长江",
    "長江",
    "闭关",
    "閉關",
    "无关",
    "無關",
    "相关",
    "相關",
    "高台",
    "高臺",
    "空山",
    "春山",
    "秋山",
    "远山",
    "遠山",
    "江水",
    "河水",
    "流水",
    "泉水",
    "白水",
    "黑水",
    "王宫",
    "王宮",
    "皇宫",
    "皇宮",
    "天宫",
    "天宮",
    "山海",
    "海山",
    "故关",
    "故關",
    "边关",
    "邊關",
    "关门",
    "關門",
}

PLACE_INVALID_CHARS = set("的兮而与與于於不无無有将將欲更又亦何也矣焉者乎且从從向去来來入出见見闻聞听聽被使令能可")
PLACE_INVALID_PREFIX = set("取独倚冻驭馳驰层翼归到入出过向望看听闻忆怀寄留问凭倚临近远高低明暗空冷暖")

# Whitelist/blacklist for secondary pattern extraction.
# rule_lexicon results are trusted more; rule_pattern is filtered by these lists.
PATTERN_PLACE_WHITELIST = {
    "关山",
    "萧关",
    "交河",
    "汀洲",
    "燕山",
    "青草湖",
    "金门",
    "江城",
}

PATTERN_PLACE_BLACKLIST = {
    "四海",
    "山河",
    "南山",
    "北山",
    "西山",
    "千山",
    "闲门",
    "边城",
}


IMAGE_LEXICON: dict[str, str] = {
    "月": "astral",
    "日": "astral",
    "星": "astral",
    "云": "weather",
    "雲": "weather",
    "风": "weather",
    "風": "weather",
    "雨": "weather",
    "雪": "weather",
    "霜": "weather",
    "山": "landscape",
    "江": "waterscape",
    "河": "waterscape",
    "湖": "waterscape",
    "海": "waterscape",
    "溪": "waterscape",
    "舟": "vehicle",
    "船": "vehicle",
    "柳": "plant",
    "梅": "plant",
    "菊": "plant",
    "竹": "plant",
    "松": "plant",
    "花": "plant",
    "雁": "animal",
    "猿": "animal",
    "鹤": "animal",
    "酒": "object",
    "灯": "object",
    "钟": "object",
    "鐘": "object",
}


NARRATIVE_TYPES: list[dict[str, Any]] = [
    {
        "id": "narr_city_memory",
        "name": "city_memory",
        "description": "都城记忆与今昔之感",
        "keywords": ["长安", "長安", "洛阳", "洛陽", "汴京", "临安", "臨安", "故国", "故國", "旧游", "京城"],
    },
    {
        "id": "narr_landscape_reflection",
        "name": "landscape_reflection",
        "description": "借景抒怀与空间映照",
        "keywords": ["山", "江", "河", "湖", "月", "云", "雲", "风", "風", "雨", "雪", "溪", "舟"],
    },
    {
        "id": "narr_travel_frontier",
        "name": "travel_frontier",
        "description": "羁旅行役与边塞行旅",
        "keywords": ["行", "旅", "客", "边", "邊", "关", "關", "塞", "驿", "驛", "征", "舟", "路"],
    },
    {
        "id": "narr_farewell_recall",
        "name": "farewell_recall",
        "description": "送别怀人与追忆",
        "keywords": ["送", "别", "別", "离", "離", "归", "歸", "忆", "憶", "怀", "懷", "友"],
    },
]


DISCOURSE_CONCEPTS: list[dict[str, str]] = [
    {
        "id": "dc_poetic_narrative",
        "name": "poetic_narrative",
        "description": "古典诗歌叙事传统",
    },
    {
        "id": "dc_urban_culture",
        "name": "urban_culture",
        "description": "都城文化与文学表达",
    },
    {
        "id": "dc_space_writing",
        "name": "space_writing",
        "description": "文学空间书写",
    },
    {
        "id": "dc_tang_song_transition",
        "name": "tang_song_transition",
        "description": "唐宋文学转型视角",
    },
]


PAPERS: list[dict[str, str]] = [
    {
        "id": "paper_urban_space",
        "title": "唐宋都城空间与诗歌叙事的关联研究",
        "year": "2024",
        "authors": "MVP Team",
        "journal": "LiteratureKG Working Paper",
    },
    {
        "id": "paper_narrative",
        "title": "古典诗歌叙事传统的知识图谱方法",
        "year": "2024",
        "authors": "MVP Team",
        "journal": "LiteratureKG Working Paper",
    },
    {
        "id": "paper_transition",
        "title": "唐宋转型视角下的意象与空间书写",
        "year": "2025",
        "authors": "MVP Team",
        "journal": "LiteratureKG Working Paper",
    },
]

RULE_VERSION = "kg_mvp_v0.6.0"
EXTRACTOR_NAME = "rule_place_image_v2"
SOURCE_CORPUS = "chinese-poetry/full_tang_song"

# Rough period hints for normalized historical places.
HISTORICAL_PERIOD_HINTS: dict[str, str] = {
    "长安": "先秦-唐",
    "洛阳": "东周-宋",
    "汴京": "北宋",
    "汴州": "唐宋",
    "临安": "南宋",
    "杭州": "唐宋",
    "金陵": "六朝-宋",
    "扬州": "唐宋",
    "成都": "汉-宋",
    "襄阳": "汉-宋",
    "姑苏": "唐宋",
    "江南": "唐宋",
    "中原": "先秦-宋",
    "关中": "先秦-宋",
    "西域": "汉-宋",
    "蜀道": "唐宋",
    "玉门关": "汉-唐",
    "阳关": "汉-唐",
    "萧关": "唐宋",
}


def find_span(text: str, fragment: str) -> tuple[int, int]:
    if not fragment:
        return -1, -1
    start = text.find(fragment)
    if start < 0:
        return -1, -1
    return start, start + len(fragment)


def canonical_geo_id(name: str) -> str:
    return "hist_" + hashlib.md5(name.encode("utf-8")).hexdigest()[:12]


def infer_historical_period(canonical_name: str, dynasty_hint: str | None = None) -> str:
    if canonical_name in HISTORICAL_PERIOD_HINTS:
        return HISTORICAL_PERIOD_HINTS[canonical_name]
    if dynasty_hint == "TANG":
        return "唐"
    if dynasty_hint == "SONG":
        return "宋"
    return "唐宋(待考)"


def build_evidence_id(
    poem_id: str,
    relation_type: str,
    target_id: str,
    evidence_text: str,
    span_start: int,
) -> str:
    key = f"{poem_id}|{relation_type}|{target_id}|{evidence_text}|{span_start}"
    return safe_id("ev", key)


def normalize_place_alias_map() -> dict[str, tuple[str, str]]:
    normalized: dict[str, tuple[str, str]] = {}
    for alias, (city_id, city_name) in PLACE_CITY_MAP.items():
        normalized[to_simplified(alias)] = (city_id, to_simplified(city_name))
    for region in GENERIC_REGION_TERMS:
        simp_region = to_simplified(region)
        region_id = "c_" + hashlib.md5(simp_region.encode("utf-8")).hexdigest()[:10]
        normalized.setdefault(simp_region, (region_id, simp_region))
    return normalized


def normalize_image_lexicon() -> dict[str, str]:
    normalized: dict[str, str] = {}
    for alias, category in IMAGE_LEXICON.items():
        normalized[to_simplified(alias)] = category
    return normalized


PLACE_ALIAS_MAP = normalize_place_alias_map()
IMAGE_ALIAS_MAP = normalize_image_lexicon()
PLACE_TERMS = sorted(PLACE_ALIAS_MAP.keys(), key=len, reverse=True)
IMAGE_TERMS = sorted(IMAGE_ALIAS_MAP.keys(), key=len, reverse=True)

for item in NARRATIVE_TYPES:
    item["description"] = to_simplified(item["description"])
    item["keywords"] = [to_simplified(k) for k in item["keywords"]]

for item in DISCOURSE_CONCEPTS:
    item["description"] = to_simplified(item["description"])

for item in PAPERS:
    item["title"] = to_simplified(item["title"])


def infer_place_type(name: str) -> str:
    if re.search(r"[江河湖海溪津浦洲渚水川]", name):
        return "waterscape"
    if re.search(r"[山岭峰岳崖谷]", name):
        return "mountain"
    if re.search(r"(州|郡|县|府|城|京|关|桥|楼|台|寺|宫|阁|门)$", name):
        return "city_space"
    return "region"


def detect_place_hits(text: str, max_hits: int = 10) -> list[dict[str, str]]:
    hits: list[dict[str, str]] = []
    seen: set[str] = set()

    for alias in PLACE_TERMS:
        if alias in text:
            city_id, city_name = PLACE_ALIAS_MAP[alias]
            key = f"{alias}|{city_id}"
            if key in seen:
                continue
            seen.add(key)
            hits.append(
                {
                    "place_name": alias,
                    "city_id": city_id,
                    "city_name": city_name,
                    "evidence_text": alias,
                    "place_type": infer_place_type(alias),
                    "source": "rule_lexicon",
                    "confidence": "0.82",
                }
            )
            if len(hits) >= max_hits:
                return hits

    clauses = re.split(r"[，。！？；、,.!?;：:]", text)
    for clause in clauses:
        clause = clause.strip()
        if len(clause) < 2:
            continue
        for pattern in (PLACE_PATTERN_CORE, PLACE_PATTERN_BUILDING, PLACE_PATTERN_WATER):
            for match in pattern.finditer(clause):
                token = to_simplified(match.group(0))
                if token in PLACE_STOPWORDS:
                    continue
                if len(token) < 2:
                    continue
                if token[0] in PLACE_INVALID_PREFIX:
                    continue
                if any(ch in PLACE_INVALID_CHARS for ch in token):
                    continue

                if token in PLACE_ALIAS_MAP:
                    city_id, city_name = PLACE_ALIAS_MAP[token]
                    confidence = "0.72"
                else:
                    if len(token) > 3:
                        continue
                    city_id = safe_id("c", token)
                    city_name = token
                    confidence = "0.58"

                key = f"{token}|{city_id}"
                if key in seen:
                    continue
                seen.add(key)
                hits.append(
                    {
                        "place_name": token,
                        "city_id": city_id,
                        "city_name": city_name,
                        "evidence_text": token,
                        "place_type": infer_place_type(token),
                        "source": "rule_pattern",
                        "confidence": confidence,
                    }
                )
                if len(hits) >= max_hits:
                    return hits

    return hits


@dataclass
class PoemRecord:
    dynasty: str
    author: str
    title: str
    content: str


def fetch_json(url: str, timeout: int = 45, retries: int = 5) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = resp.read()
            return json.loads(payload.decode("utf-8"))
        except (
            urllib.error.URLError,
            urllib.error.HTTPError,
            TimeoutError,
            ConnectionError,
            http.client.IncompleteRead,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc
            if attempt >= retries:
                break
            wait = min(0.8 * (2 ** (attempt - 1)), 6.0)
            print(f"[WARN] fetch retry {attempt}/{retries} for {url}: {exc}")
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch URL after retries: {url}. Last error: {last_error}")


def safe_id(prefix: str, text: str) -> str:
    digest = hashlib.md5(text.encode("utf-8")).hexdigest()[:10]
    return f"{prefix}_{digest}"


def clean_text(text: str) -> str:
    # Keep punctuation, but normalize whitespace.
    text = re.sub(r"\s+", "", text)
    return text.strip()


def parse_poem_obj(obj: dict[str, Any], dynasty: str) -> PoemRecord | None:
    author = to_simplified(str(obj.get("author", "")).strip())
    title = to_simplified(str(obj.get("title", "")).strip())
    raw_paragraphs = obj.get("paragraphs", "")

    if isinstance(raw_paragraphs, list):
        content = "".join(str(x) for x in raw_paragraphs)
    else:
        content = str(raw_paragraphs)

    content = to_simplified(clean_text(content))
    if not author or not title or len(content) < 8:
        return None

    return PoemRecord(dynasty=dynasty, author=author, title=title, content=content)


def download_with_retries(url: str, target: Path, timeout: int = 120, retries: int = 5) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp, tmp.open("wb") as f:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
            tmp.replace(target)
            return
        except Exception as exc:
            last_error = exc
            tmp.unlink(missing_ok=True)
            if attempt >= retries:
                break
            wait = min(1.0 * (2 ** (attempt - 1)), 8.0)
            print(f"[WARN] download retry {attempt}/{retries} for {url}: {exc}")
            time.sleep(wait)

    raise RuntimeError(f"Failed to download {url}. Last error: {last_error}")


def find_full_tang_song_dir() -> str:
    root_items = fetch_json(f"{API_ROOT}/contents")
    if not isinstance(root_items, list):
        raise RuntimeError("Unexpected API response for repository root.")

    for item in root_items:
        if item.get("type") != "dir":
            continue
        dir_name = item.get("name", "")
        encoded = urllib.parse.quote(dir_name, safe="/")
        try:
            children = fetch_json(f"{API_ROOT}/contents/{encoded}")
        except Exception:
            continue
        if not isinstance(children, list):
            continue
        names = [str(c.get("name", "")) for c in children if c.get("type") == "file"]
        if any(n.startswith("poet.tang.") for n in names) and any(
            n.startswith("poet.song.") for n in names
        ):
            return dir_name

    raise RuntimeError("Could not locate directory containing poet.tang.* and poet.song.*.")


def list_poetry_files(dir_name: str, prefix: str) -> list[str]:
    encoded = urllib.parse.quote(dir_name, safe="/")
    children = fetch_json(f"{API_ROOT}/contents/{encoded}")
    if not isinstance(children, list):
        raise RuntimeError(f"Unexpected directory API response: {dir_name}")

    files = [str(c["name"]) for c in children if c.get("type") == "file" and str(c.get("name", "")).startswith(prefix)]

    def offset(name: str) -> int:
        m = re.search(r"\.(\d+)\.json$", name)
        return int(m.group(1)) if m else 0

    files.sort(key=offset)
    return files


def download_file_via_api(path: str, target: Path) -> None:
    encoded_path = urllib.parse.quote(path, safe="/")
    file_obj = fetch_json(f"{API_ROOT}/contents/{encoded_path}")
    if not isinstance(file_obj, dict):
        raise RuntimeError(f"Unexpected file API response: {path}")
    if file_obj.get("encoding") != "base64":
        raise RuntimeError(f"Unsupported encoding for {path}: {file_obj.get('encoding')}")

    payload = base64.b64decode(file_obj.get("content", ""))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)


def load_records_from_api(
    cache_dir: Path,
    tang_raw_target: int,
    song_raw_target: int,
    max_files_per_dynasty: int,
    pause_seconds: float,
) -> list[PoemRecord]:
    dir_name = find_full_tang_song_dir()
    print(f"[INFO] Found source directory in repo: {dir_name}")

    tang_files = list_poetry_files(dir_name, "poet.tang.")
    song_files = list_poetry_files(dir_name, "poet.song.")
    if not tang_files or not song_files:
        raise RuntimeError("No tang/song source files found.")

    collected: list[PoemRecord] = []
    targets = [("tang", tang_files, tang_raw_target), ("song", song_files, song_raw_target)]
    for dynasty, files, raw_target in targets:
        raw_count = 0
        used_files = 0
        for filename in files:
            if raw_count >= raw_target:
                break
            if used_files >= max_files_per_dynasty:
                break

            rel_path = f"{dir_name}/{filename}"
            local_path = cache_dir / filename
            if not local_path.exists():
                print(f"[INFO] Downloading {rel_path}")
                download_file_via_api(rel_path, local_path)
                if pause_seconds > 0:
                    time.sleep(pause_seconds)
            else:
                print(f"[INFO] Using cache {filename}")
            try:
                data = json.loads(local_path.read_text(encoding="utf-8"))
            except Exception:
                # Damaged cache can happen if previous run was interrupted.
                print(f"[WARN] Bad cache detected. Re-downloading {rel_path}")
                local_path.unlink(missing_ok=True)
                download_file_via_api(rel_path, local_path)
                data = json.loads(local_path.read_text(encoding="utf-8"))
            for obj in data:
                rec = parse_poem_obj(obj, dynasty.upper())
                if rec is not None:
                    collected.append(rec)
                    raw_count += 1
            used_files += 1

        print(f"[INFO] {dynasty.upper()} raw poems collected: {raw_count}")

    return collected


def load_records_from_zip(
    cache_dir: Path,
    tang_raw_target: int,
    song_raw_target: int,
    max_files_per_dynasty: int,
) -> list[PoemRecord]:
    zip_path = cache_dir / "chinese-poetry-master.zip"
    if not zip_path.exists():
        print(f"[INFO] Downloading dataset zip: {CODELOAD_ZIP_URL}")
        download_with_retries(CODELOAD_ZIP_URL, zip_path, timeout=180, retries=5)
    else:
        print(f"[INFO] Using cached zip: {zip_path}")

    collected: list[PoemRecord] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        tang_members = [
            n
            for n in names
            if re.search(r"/全唐诗/poet\.tang\.\d+\.json$", n)
        ]
        song_members = [
            n
            for n in names
            if re.search(r"/全唐诗/poet\.song\.\d+\.json$", n)
        ]

        if not tang_members or not song_members:
            raise RuntimeError("Zip source does not contain poet.tang.* or poet.song.* json files.")

        def offset(member: str) -> int:
            m = re.search(r"\.(\d+)\.json$", member)
            return int(m.group(1)) if m else 0

        tang_members.sort(key=offset)
        song_members.sort(key=offset)

        targets = [
            ("TANG", tang_members, tang_raw_target),
            ("SONG", song_members, song_raw_target),
        ]
        for dynasty, members, raw_target in targets:
            raw_count = 0
            used = 0
            for member in members:
                if raw_count >= raw_target or used >= max_files_per_dynasty:
                    break
                data = json.loads(zf.read(member).decode("utf-8"))
                for obj in data:
                    rec = parse_poem_obj(obj, dynasty)
                    if rec is not None:
                        collected.append(rec)
                        raw_count += 1
                used += 1
                print(f"[INFO] {dynasty} loaded {member} (raw_count={raw_count})")
            print(f"[INFO] {dynasty} raw poems collected: {raw_count}")

    return collected


def load_records_from_local(local_dir: Path) -> list[PoemRecord]:
    if not local_dir.exists():
        raise FileNotFoundError(f"Local source directory not found: {local_dir}")

    files = sorted(local_dir.rglob("poet.tang.*.json")) + sorted(local_dir.rglob("poet.song.*.json"))
    if not files:
        raise RuntimeError(
            "No source files found. Expected files like poet.tang.0.json / poet.song.0.json."
        )

    records: list[PoemRecord] = []
    for f in files:
        dynasty = "TANG" if ".tang." in f.name else "SONG"
        data = json.loads(f.read_text(encoding="utf-8"))
        for obj in data:
            rec = parse_poem_obj(obj, dynasty)
            if rec is not None:
                records.append(rec)
    return records


def choose_poets(records: list[PoemRecord], tang_poets: int, song_poets: int) -> dict[str, list[str]]:
    by_dynasty: dict[str, Counter[str]] = {"TANG": Counter(), "SONG": Counter()}
    for r in records:
        by_dynasty[r.dynasty][r.author] += 1

    selected = {
        "TANG": [name for name, _ in by_dynasty["TANG"].most_common(tang_poets)],
        "SONG": [name for name, _ in by_dynasty["SONG"].most_common(song_poets)],
    }
    return selected


def sample_poems_balanced(
    records: list[PoemRecord], selected_authors: list[str], target_count: int, seed: int
) -> list[PoemRecord]:
    groups: dict[str, list[PoemRecord]] = defaultdict(list)
    for r in records:
        if r.author in selected_authors:
            groups[r.author].append(r)

    rng = random.Random(seed)
    for author in groups:
        groups[author].sort(key=lambda x: (x.title, x.content[:32]))
        rng.shuffle(groups[author])

    picked: list[PoemRecord] = []
    if not selected_authors:
        return picked

    quota = max(1, target_count // len(selected_authors))
    positions = {a: 0 for a in selected_authors}

    for a in selected_authors:
        author_items = groups.get(a, [])
        take = min(quota, len(author_items))
        picked.extend(author_items[:take])
        positions[a] = take

    while len(picked) < target_count:
        progress = False
        for a in selected_authors:
            author_items = groups.get(a, [])
            idx = positions[a]
            if idx < len(author_items):
                picked.append(author_items[idx])
                positions[a] += 1
                progress = True
                if len(picked) >= target_count:
                    break
        if not progress:
            break

    return picked[:target_count]


def detect_terms(text: str, terms: list[str], max_hits: int) -> list[str]:
    hits: list[str] = []
    for term in terms:
        if term in text:
            hits.append(term)
            if len(hits) >= max_hits:
                break
    return hits


def detect_narrative(text: str) -> list[str]:
    scored: list[tuple[str, int]] = []
    for n in NARRATIVE_TYPES:
        score = sum(text.count(k) for k in n["keywords"])
        scored.append((n["id"], score))
    scored.sort(key=lambda x: x[1], reverse=True)

    top_id, top_score = scored[0]
    if top_score == 0:
        return ["narr_landscape_reflection"]

    result = [top_id]
    if len(scored) > 1 and scored[1][1] >= 2:
        result.append(scored[1][0])
    return result


def detect_discourse(dynasty: str, places_found: int, narratives: list[str]) -> list[str]:
    concepts = {"dc_poetic_narrative"}
    if places_found > 0:
        concepts.add("dc_space_writing")
    if "narr_city_memory" in narratives:
        concepts.add("dc_urban_culture")
    if dynasty == "SONG":
        concepts.add("dc_tang_song_transition")
    return sorted(concepts)


def write_csv(path: Path, headers: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in headers})


def build_dataset(
    records: list[PoemRecord],
    out_dir: Path,
    target_poets: int,
    tang_poems: int,
    song_poems: int,
    seed: int,
) -> None:
    min_pattern_place_freq = 3

    tang_poets = target_poets // 2
    song_poets = target_poets - tang_poets
    selected = choose_poets(records, tang_poets=tang_poets, song_poets=song_poets)

    tang_records = [r for r in records if r.dynasty == "TANG"]
    song_records = [r for r in records if r.dynasty == "SONG"]

    tang_sample = sample_poems_balanced(
        tang_records, selected["TANG"], target_count=tang_poems, seed=seed
    )
    song_sample = sample_poems_balanced(
        song_records, selected["SONG"], target_count=song_poems, seed=seed + 1
    )
    poems = tang_sample + song_sample

    print(f"[INFO] Selected poets: TANG={len(selected['TANG'])}, SONG={len(selected['SONG'])}")
    print(f"[INFO] Sampled poems: TANG={len(tang_sample)}, SONG={len(song_sample)}")

    poet_rows: list[dict[str, Any]] = []
    poet_id_map: dict[tuple[str, str], str] = {}
    for dynasty, authors in selected.items():
        era = "TANG" if dynasty == "TANG" else "SONG"
        for name in authors:
            pid = safe_id(f"p_{dynasty.lower()}", name)
            poet_id_map[(dynasty, name)] = pid
            poet_rows.append(
                {
                    "id": pid,
                    "name": name,
                    "era": era,
                    "birth_year": "",
                    "death_year": "",
                    "notes": "source: chinese-poetry",
                }
            )

    poem_rows: list[dict[str, Any]] = []
    rel_wrote: list[dict[str, Any]] = []
    rel_created_in: list[dict[str, Any]] = []
    rel_mentions_place: list[dict[str, Any]] = []
    rel_uses_image: list[dict[str, Any]] = []
    rel_has_narrative: list[dict[str, Any]] = []
    rel_embodies_discourse: list[dict[str, Any]] = []
    rel_discussed_in: list[dict[str, Any]] = []
    rel_normalized_to: list[dict[str, Any]] = []
    rel_canon_located_in: list[dict[str, Any]] = []
    rel_has_evidence: list[dict[str, Any]] = []
    rel_evidence_supports_place: list[dict[str, Any]] = []
    rel_evidence_supports_image: list[dict[str, Any]] = []

    places: dict[str, dict[str, Any]] = {}
    cities: dict[str, dict[str, Any]] = {}
    images: dict[str, dict[str, Any]] = {}
    canonical_places: dict[str, dict[str, Any]] = {}
    evidences: dict[str, dict[str, Any]] = {}
    raw_place_mentions: list[dict[str, Any]] = []
    normalized_edges_seen: set[tuple[str, str]] = set()
    canon_located_seen: set[tuple[str, str]] = set()
    has_evidence_seen: set[tuple[str, str]] = set()
    supports_place_seen: set[tuple[str, str]] = set()
    supports_image_seen: set[tuple[str, str]] = set()

    for idx, p in enumerate(poems, start=1):
        poem_key = f"{p.dynasty}|{p.author}|{p.title}|{idx}|{p.content[:24]}"
        poem_id = safe_id("m", poem_key)
        era_id = "TANG" if p.dynasty == "TANG" else "SONG"

        poem_rows.append(
            {
                "id": poem_id,
                "title": to_simplified(p.title),
                "content": to_simplified(p.content),
                "genre": "shi" if p.dynasty == "TANG" else "shi_ci",
                "source": "chinese-poetry",
            }
        )

        rel_wrote.append(
            {
                "poet_id": poet_id_map[(p.dynasty, p.author)],
                "poem_id": poem_id,
                "source": "chinese-poetry",
                "confidence": 1.0,
            }
        )
        rel_created_in.append(
            {
                "poem_id": poem_id,
                "era_id": era_id,
                "source": "rule_dynasty",
                "confidence": 1.0,
            }
        )

        place_hits = detect_place_hits(p.content, max_hits=8)
        for hit in place_hits:
            span_start, span_end = find_span(p.content, to_simplified(hit["evidence_text"]))
            raw_place_mentions.append(
                {
                    "poem_id": poem_id,
                    "dynasty": p.dynasty,
                    "place_name": to_simplified(hit["place_name"]),
                    "city_id": hit["city_id"],
                    "city_name": to_simplified(hit["city_name"]),
                    "evidence_text": to_simplified(hit["evidence_text"]),
                    "span_start": span_start,
                    "span_end": span_end,
                    "place_type": hit["place_type"],
                    "source": hit["source"],
                    "confidence": float(hit["confidence"]),
                    "rule_version": RULE_VERSION,
                    "extractor": EXTRACTOR_NAME,
                    "source_file": SOURCE_CORPUS,
                    "source_record_id": poem_id,
                }
            )

        image_hits = detect_terms(p.content, IMAGE_TERMS, max_hits=6)
        for term in image_hits:
            term = to_simplified(term)
            image_id = safe_id("img", term)
            span_start, span_end = find_span(p.content, term)
            if image_id not in images:
                images[image_id] = {
                    "id": image_id,
                    "name": term,
                    "category": IMAGE_ALIAS_MAP[term],
                }
            evidence_id = build_evidence_id(
                poem_id=poem_id,
                relation_type="USES_IMAGE",
                target_id=image_id,
                evidence_text=term,
                span_start=span_start,
            )
            rel_uses_image.append(
                {
                    "poem_id": poem_id,
                    "image_id": image_id,
                    "evidence_text": term,
                    "source": "rule_lexicon",
                    "confidence": 0.75,
                    "evidence_id": evidence_id,
                    "rule_version": RULE_VERSION,
                    "extractor": EXTRACTOR_NAME,
                    "source_file": SOURCE_CORPUS,
                    "source_record_id": poem_id,
                    "match_span_start": span_start,
                    "match_span_end": span_end,
                }
            )

        narratives = detect_narrative(p.content)
        for nid in narratives:
            rel_has_narrative.append(
                {
                    "poem_id": poem_id,
                    "narrative_type_id": nid,
                    "source": "rule_keywords",
                    "confidence": 0.7,
                }
            )

        concepts = detect_discourse(p.dynasty, len(place_hits), narratives)
        for cid in concepts:
            rel_embodies_discourse.append(
                {
                    "poem_id": poem_id,
                    "concept_id": cid,
                    "evidence_text": ",".join(narratives),
                    "source": "rule_inference",
                    "confidence": 0.65,
                }
            )

        # Attach one synthetic reference paper for demo retrieval.
        if "dc_urban_culture" in concepts:
            paper_id = "paper_urban_space"
        elif "dc_tang_song_transition" in concepts:
            paper_id = "paper_transition"
        else:
            paper_id = "paper_narrative"

        rel_discussed_in.append(
            {
                "poem_id": poem_id,
                "paper_id": paper_id,
                "source": "mvp_mapping",
                "confidence": 0.6,
            }
        )

    pattern_freq = Counter(
        m["place_name"] for m in raw_place_mentions if m["source"] == "rule_pattern"
    )
    for mention in raw_place_mentions:
        if (
            mention["source"] == "rule_pattern"
            and pattern_freq[mention["place_name"]] < min_pattern_place_freq
        ):
            continue
        if mention["source"] == "rule_pattern":
            place_name = mention["place_name"]
            if place_name in PATTERN_PLACE_BLACKLIST:
                continue
            if (
                place_name not in PATTERN_PLACE_WHITELIST
                and place_name not in PLACE_ALIAS_MAP
            ):
                continue

        place_name = mention["place_name"]
        city_id = mention["city_id"]
        city_name = mention["city_name"]
        place_id = safe_id("pl", place_name)
        canonical_name = city_name if place_name in PLACE_ALIAS_MAP else place_name
        canonical_place_id = safe_id("pcn", f"{city_id}|{canonical_name}")

        if city_id not in cities:
            cities[city_id] = {
                "id": city_id,
                "name": city_name,
                "modern_location": "",
            }
        if place_id not in places:
            places[place_id] = {
                "id": place_id,
                "name": place_name,
                "type": mention["place_type"],
                "notes": "",
                "city_id": city_id,
            }
        if canonical_place_id not in canonical_places:
            canonical_places[canonical_place_id] = {
                "id": canonical_place_id,
                "name": canonical_name,
                "period": infer_historical_period(canonical_name, mention.get("dynasty")),
                "geo_id": canonical_geo_id(canonical_name),
                "type": mention["place_type"],
                "notes": "auto_normalized",
            }

        evidence_id = build_evidence_id(
            poem_id=mention["poem_id"],
            relation_type="MENTIONS_PLACE",
            target_id=place_id,
            evidence_text=mention["evidence_text"],
            span_start=mention["span_start"],
        )
        rel_mentions_place.append(
            {
                "poem_id": mention["poem_id"],
                "place_id": place_id,
                "evidence_text": mention["evidence_text"],
                "source": mention["source"],
                "confidence": mention["confidence"],
                "evidence_id": evidence_id,
                "rule_version": mention["rule_version"],
                "extractor": mention["extractor"],
                "source_file": mention["source_file"],
                "source_record_id": mention["source_record_id"],
                "match_span_start": mention["span_start"],
                "match_span_end": mention["span_end"],
            }
        )
        if evidence_id not in evidences:
            evidences[evidence_id] = {
                "id": evidence_id,
                "evidence_type": "text_span",
                "span_text": mention["evidence_text"],
                "span_start": mention["span_start"],
                "span_end": mention["span_end"],
                "rule_version": mention["rule_version"],
                "extractor": mention["extractor"],
                "source_file": mention["source_file"],
                "source_record_id": mention["source_record_id"],
                "confidence": mention["confidence"],
            }

        norm_key = (place_id, canonical_place_id)
        if norm_key not in normalized_edges_seen:
            normalized_edges_seen.add(norm_key)
            rel_normalized_to.append(
                {
                    "place_id": place_id,
                    "canonical_place_id": canonical_place_id,
                    "method": "alias_city_map" if place_name in PLACE_ALIAS_MAP else "pattern_infer",
                    "source": "normalization_rule",
                    "confidence": 0.9 if place_name in PLACE_ALIAS_MAP else 0.7,
                }
            )

        canon_loc_key = (canonical_place_id, city_id)
        if canon_loc_key not in canon_located_seen:
            canon_located_seen.add(canon_loc_key)
            rel_canon_located_in.append(
                {
                    "canonical_place_id": canonical_place_id,
                    "city_id": city_id,
                    "source": "normalization_rule",
                    "confidence": 0.9,
                }
            )

        has_ev_key = (mention["poem_id"], evidence_id)
        if has_ev_key not in has_evidence_seen:
            has_evidence_seen.add(has_ev_key)
            rel_has_evidence.append(
                {
                    "poem_id": mention["poem_id"],
                    "evidence_id": evidence_id,
                    "source": mention["source"],
                }
            )

        support_key = (evidence_id, place_id)
        if support_key not in supports_place_seen:
            supports_place_seen.add(support_key)
            rel_evidence_supports_place.append(
                {
                    "evidence_id": evidence_id,
                    "place_id": place_id,
                    "source": mention["source"],
                }
            )

    for rel in rel_uses_image:
        evidence_id = rel["evidence_id"]
        if evidence_id not in evidences:
            evidences[evidence_id] = {
                "id": evidence_id,
                "evidence_type": "text_span",
                "span_text": rel["evidence_text"],
                "span_start": rel["match_span_start"],
                "span_end": rel["match_span_end"],
                "rule_version": rel["rule_version"],
                "extractor": rel["extractor"],
                "source_file": rel["source_file"],
                "source_record_id": rel["source_record_id"],
                "confidence": rel["confidence"],
            }

        has_ev_key = (rel["poem_id"], evidence_id)
        if has_ev_key not in has_evidence_seen:
            has_evidence_seen.add(has_ev_key)
            rel_has_evidence.append(
                {
                    "poem_id": rel["poem_id"],
                    "evidence_id": evidence_id,
                    "source": rel["source"],
                }
            )

        support_key = (evidence_id, rel["image_id"])
        if support_key not in supports_image_seen:
            supports_image_seen.add(support_key)
            rel_evidence_supports_image.append(
                {
                    "evidence_id": evidence_id,
                    "image_id": rel["image_id"],
                    "source": rel["source"],
                }
            )

    rel_located_in = [
        {"place_id": p["id"], "city_id": p["city_id"], "source": "rule_map", "confidence": 1.0}
        for p in places.values()
    ]

    narrative_rows = [{k: v for k, v in item.items() if k != "keywords"} for item in NARRATIVE_TYPES]
    discourse_rows = DISCOURSE_CONCEPTS
    paper_rows = PAPERS

    eras = [
        {"id": "TANG", "name": "Tang", "start_year": 618, "end_year": 907},
        {"id": "SONG", "name": "Song", "start_year": 960, "end_year": 1279},
    ]

    write_csv(out_dir / "eras.csv", ["id", "name", "start_year", "end_year"], eras)
    write_csv(
        out_dir / "poets.csv",
        ["id", "name", "era", "birth_year", "death_year", "notes"],
        sorted(poet_rows, key=lambda x: x["id"]),
    )
    write_csv(
        out_dir / "poems.csv",
        ["id", "title", "content", "genre", "source"],
        poem_rows,
    )
    write_csv(
        out_dir / "cities.csv",
        ["id", "name", "modern_location"],
        sorted(cities.values(), key=lambda x: x["id"]),
    )
    place_rows = [
        {"id": p["id"], "name": p["name"], "type": p["type"], "notes": p["notes"]}
        for p in places.values()
    ]
    canonical_place_rows = sorted(canonical_places.values(), key=lambda x: x["id"])
    evidence_rows = sorted(evidences.values(), key=lambda x: x["id"])
    write_csv(out_dir / "places.csv", ["id", "name", "type", "notes"], sorted(place_rows, key=lambda x: x["id"]))
    write_csv(
        out_dir / "canonical_places.csv",
        ["id", "name", "period", "geo_id", "type", "notes"],
        canonical_place_rows,
    )
    write_csv(
        out_dir / "evidences.csv",
        [
            "id",
            "evidence_type",
            "span_text",
            "span_start",
            "span_end",
            "rule_version",
            "extractor",
            "source_file",
            "source_record_id",
            "confidence",
        ],
        evidence_rows,
    )
    write_csv(out_dir / "images.csv", ["id", "name", "category"], sorted(images.values(), key=lambda x: x["id"]))
    write_csv(
        out_dir / "narrative_types.csv",
        ["id", "name", "description"],
        narrative_rows,
    )
    write_csv(
        out_dir / "discourse_concepts.csv",
        ["id", "name", "description"],
        discourse_rows,
    )
    write_csv(out_dir / "papers.csv", ["id", "title", "year", "authors", "journal"], paper_rows)
    write_csv(out_dir / "rel_wrote.csv", ["poet_id", "poem_id", "source", "confidence"], rel_wrote)
    write_csv(out_dir / "rel_created_in.csv", ["poem_id", "era_id", "source", "confidence"], rel_created_in)
    write_csv(
        out_dir / "rel_mentions_place.csv",
        [
            "poem_id",
            "place_id",
            "evidence_text",
            "source",
            "confidence",
            "evidence_id",
            "rule_version",
            "extractor",
            "source_file",
            "source_record_id",
            "match_span_start",
            "match_span_end",
        ],
        rel_mentions_place,
    )
    write_csv(
        out_dir / "rel_uses_image.csv",
        [
            "poem_id",
            "image_id",
            "evidence_text",
            "source",
            "confidence",
            "evidence_id",
            "rule_version",
            "extractor",
            "source_file",
            "source_record_id",
            "match_span_start",
            "match_span_end",
        ],
        rel_uses_image,
    )
    write_csv(
        out_dir / "rel_has_narrative.csv",
        ["poem_id", "narrative_type_id", "source", "confidence"],
        rel_has_narrative,
    )
    write_csv(
        out_dir / "rel_embodies_discourse.csv",
        ["poem_id", "concept_id", "evidence_text", "source", "confidence"],
        rel_embodies_discourse,
    )
    write_csv(
        out_dir / "rel_discussed_in.csv",
        ["poem_id", "paper_id", "source", "confidence"],
        rel_discussed_in,
    )
    write_csv(
        out_dir / "rel_located_in.csv",
        ["place_id", "city_id", "source", "confidence"],
        rel_located_in,
    )
    write_csv(
        out_dir / "rel_normalized_to.csv",
        ["place_id", "canonical_place_id", "method", "source", "confidence"],
        rel_normalized_to,
    )
    write_csv(
        out_dir / "rel_canon_located_in.csv",
        ["canonical_place_id", "city_id", "source", "confidence"],
        rel_canon_located_in,
    )
    write_csv(
        out_dir / "rel_has_evidence.csv",
        ["poem_id", "evidence_id", "source"],
        rel_has_evidence,
    )
    write_csv(
        out_dir / "rel_evidence_supports_place.csv",
        ["evidence_id", "place_id", "source"],
        rel_evidence_supports_place,
    )
    write_csv(
        out_dir / "rel_evidence_supports_image.csv",
        ["evidence_id", "image_id", "source"],
        rel_evidence_supports_image,
    )

    print("\n[SUMMARY]")
    print(
        f"poets={len(poet_rows)} poems={len(poem_rows)} places={len(place_rows)} "
        f"canonical_places={len(canonical_place_rows)} cities={len(cities)} images={len(images)} "
        f"evidences={len(evidence_rows)}"
    )
    print(
        f"rel_wrote={len(rel_wrote)} rel_mentions_place={len(rel_mentions_place)} "
        f"rel_uses_image={len(rel_uses_image)} rel_normalized_to={len(rel_normalized_to)} "
        f"rel_has_evidence={len(rel_has_evidence)}"
    )
    print(f"Output directory: {out_dir.resolve()}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build MVP LiteratureKG CSV files from Tang/Song poetry source."
    )
    parser.add_argument(
        "--source",
        choices=["zip", "api", "local"],
        default="zip",
        help="Use codeload zip, GitHub API, or local json files (default: zip).",
    )
    parser.add_argument(
        "--local-dir",
        default="data/source/chinese-poetry",
        help="Local source root containing poet.tang.*.json and poet.song.*.json",
    )
    parser.add_argument("--output-dir", default="data/input", help="Output CSV directory.")
    parser.add_argument("--cache-dir", default="data/cache/chinese-poetry", help="Cache directory for API files.")
    parser.add_argument("--target-poets", type=int, default=50, help="Target number of poets.")
    parser.add_argument("--tang-poems", type=int, default=700, help="Target Tang poem count.")
    parser.add_argument("--song-poems", type=int, default=800, help="Target Song poem count.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument(
        "--max-files-per-dynasty",
        type=int,
        default=15,
        help="Max source files to read per dynasty when source=api.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.2,
        help="Sleep interval between API file requests.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.output_dir)

    if args.source == "zip":
        records = load_records_from_zip(
            cache_dir=Path(args.cache_dir),
            tang_raw_target=max(args.tang_poems * 3, 4000),
            song_raw_target=max(args.song_poems * 3, 5000),
            max_files_per_dynasty=args.max_files_per_dynasty,
        )
    elif args.source == "api":
        records = load_records_from_api(
            cache_dir=Path(args.cache_dir),
            tang_raw_target=max(args.tang_poems * 3, 4000),
            song_raw_target=max(args.song_poems * 3, 5000),
            max_files_per_dynasty=args.max_files_per_dynasty,
            pause_seconds=args.pause_seconds,
        )
    else:
        records = load_records_from_local(Path(args.local_dir))

    if not records:
        raise RuntimeError("No valid poem records loaded.")

    build_dataset(
        records=records,
        out_dir=out_dir,
        target_poets=args.target_poets,
        tang_poems=args.tang_poems,
        song_poems=args.song_poems,
        seed=args.seed,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
