#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_knowledge_base.py
免疫学文献知识库构建脚本（脚本 A：数据整理与结构化）

功能：
  Step 1 — 扫描 downloaded_index，读取 JSON 元数据，写入 papers 表
  Step 2 — 免疫指标关键词匹配，写入 paper_indicators 表
  Step 3 — 查询期刊影响因子（OpenAlex API），写入 journal_info 表
  Step 4 — 查询论文引用次数（OpenAlex API），断点续传
  Step 5 — 生成统计摘要

用法：
  python build_knowledge_base.py --data-dir ./downloaded_index --keywords ./immune_keywords.txt --db ./immune_kb.db
  python build_knowledge_base.py --steps 1
  python build_knowledge_base.py --steps 1,2
  python build_knowledge_base.py --steps 3 --email luchichen000@gmail.com
  python build_knowledge_base.py --steps 4
  python build_knowledge_base.py --steps 5
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("[警告] tqdm 未安装，将不显示进度条。运行 pip install tqdm 安装。")


# ─────────────────────────────────────────────
# 数据库初始化
# ─────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS papers (
    pmid TEXT PRIMARY KEY,
    title TEXT,
    doi TEXT,
    journal TEXT,
    volume TEXT,
    issue TEXT,
    year INTEGER,
    month TEXT,
    authors TEXT,
    abstract TEXT,
    matched_keyword TEXT,
    fulltext_source TEXT,
    has_fulltext INTEGER DEFAULT 0,
    word_count INTEGER,
    impact_factor REAL,
    citation_count INTEGER,
    study_type TEXT,
    species TEXT,
    age_group TEXT,
    sample_size INTEGER,
    country TEXT,
    pub_type TEXT,
    language TEXT,
    llm_disease_tags TEXT,
    llm_mechanism TEXT,
    llm_conclusion TEXT,
    llm_reference_range TEXT,
    llm_reference_population TEXT,
    llm_reference_sample_size INTEGER,
    mesh_terms TEXT,
    keywords TEXT,
    embed_status INTEGER DEFAULT 0,
    llm_annotated_at TEXT,
    llm_model TEXT,
    embedded_at TEXT,
    vector_collection TEXT,
    pubmed_url TEXT,
    doi_url TEXT,
    download_date TEXT,
    md_file_path TEXT,
    json_file_path TEXT,
    indicator_dir TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS paper_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pmid TEXT,
    indicator TEXT,
    matched_alias TEXT,
    match_location TEXT,
    FOREIGN KEY (pmid) REFERENCES papers(pmid)
);
CREATE INDEX IF NOT EXISTS idx_pi_pmid ON paper_indicators(pmid);
CREATE INDEX IF NOT EXISTS idx_pi_indicator ON paper_indicators(indicator);

CREATE TABLE IF NOT EXISTS paper_diseases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pmid TEXT,
    disease TEXT,
    FOREIGN KEY (pmid) REFERENCES papers(pmid)
);
CREATE INDEX IF NOT EXISTS idx_pd_pmid ON paper_diseases(pmid);
CREATE INDEX IF NOT EXISTS idx_pd_disease ON paper_diseases(disease);

CREATE TABLE IF NOT EXISTS paper_mechanisms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pmid TEXT,
    mechanism TEXT,
    FOREIGN KEY (pmid) REFERENCES papers(pmid)
);

CREATE TABLE IF NOT EXISTS relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pmid TEXT,
    subject TEXT,
    predicate TEXT,
    object TEXT,
    FOREIGN KEY (pmid) REFERENCES papers(pmid)
);
CREATE INDEX IF NOT EXISTS idx_rel_subject ON relationships(subject);
CREATE INDEX IF NOT EXISTS idx_rel_object ON relationships(object);

CREATE TABLE IF NOT EXISTS journal_info (
    journal_name TEXT PRIMARY KEY,
    impact_factor REAL,
    category TEXT,
    issn TEXT,
    h_index INTEGER
);

CREATE TABLE IF NOT EXISTS processing_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pmid TEXT,
    step TEXT,
    status TEXT,
    message TEXT,
    processed_at TEXT DEFAULT (datetime('now'))
);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    """初始化数据库，创建所有表（如不存在）。"""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()
    print(f"[数据库] 已连接/创建数据库：{db_path}")
    return conn


def log_error(conn: sqlite3.Connection, pmid: str, step: str, message: str):
    """记录错误到 processing_log 表。"""
    try:
        conn.execute(
            "INSERT INTO processing_log (pmid, step, status, message) VALUES (?, ?, 'error', ?)",
            (pmid, step, message[:2000])
        )
    except Exception:
        pass


# ─────────────────────────────────────────────
# 进度条辅助
# ─────────────────────────────────────────────

def make_tqdm(iterable, **kwargs):
    if TQDM_AVAILABLE:
        return tqdm(iterable, **kwargs)
    # 简单的无进度条回退
    total = kwargs.get("total", None)
    desc = kwargs.get("desc", "")
    if desc:
        print(f"[进度] {desc} ({'共 ' + str(total) + ' 条' if total else ''})")
    return iterable


# ─────────────────────────────────────────────
# Step 1：扫描文件，读取 JSON 元数据
# ─────────────────────────────────────────────

def count_words(text: str) -> int:
    """
    计算文本字数：
    - 中文按字符数计
    - 英文按空格分隔的单词数计
    """
    if not text:
        return 0
    # 统计中文字符
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff\u3400-\u4dbf]', text))
    # 去掉中文后，统计英文单词
    text_no_cn = re.sub(r'[\u4e00-\u9fff\u3400-\u4dbf]', ' ', text)
    english_words = len(text_no_cn.split())
    return chinese_chars + english_words


def find_md_file(json_path: Path) -> Optional[Path]:
    """根据 JSON 文件路径找对应的 MD 文件（去掉 _metadata.json 后缀，加 .md）。"""
    stem = json_path.stem  # 例：40215168_Title..._metadata
    if stem.endswith("_metadata"):
        base = stem[:-len("_metadata")]
    else:
        base = stem
    md_path = json_path.parent / (base + ".md")
    if md_path.exists():
        return md_path
    # 尝试不区分大小写
    for f in json_path.parent.iterdir():
        if f.suffix.lower() == ".md" and f.stem.lower() == base.lower():
            return f
    return None


def scan_and_insert_papers(conn: sqlite3.Connection, data_dir: Path):
    """
    Step 1：递归扫描 data_dir，找出所有 _metadata.json 文件，
    解析后写入 papers 表（已存在的 PMID 跳过）。
    """
    print("\n[Step 1] 开始扫描文件目录，读取 JSON 元数据...")

    # 收集所有 _metadata.json 文件
    json_files = list(data_dir.rglob("*_metadata.json"))
    # 排除根目录下以 _ 开头的报告/日志文件（非文献）
    json_files = [f for f in json_files if not f.name.startswith("_")]

    total = len(json_files)
    print(f"[Step 1] 共发现 {total} 个元数据文件")

    # 获取已存在的 PMID 集合
    existing_pmids = set(
        row[0] for row in conn.execute("SELECT pmid FROM papers")
    )
    print(f"[Step 1] 数据库中已有 {len(existing_pmids)} 条记录，跳过重复")

    inserted = 0
    skipped = 0
    errors = 0
    batch = []
    BATCH_SIZE = 1000

    pbar = make_tqdm(json_files, total=total, desc="Step 1 读取元数据", unit="篇")

    for json_path in pbar:
        try:
            # 从路径解析 indicator_dir（data_dir 下第一层子目录名）
            try:
                rel = json_path.relative_to(data_dir)
                indicator_dir = rel.parts[0] if len(rel.parts) > 1 else ""
            except ValueError:
                indicator_dir = ""

            # 读取 JSON
            with open(json_path, "r", encoding="utf-8") as f:
                meta = json.load(f)

            pmid = str(meta.get("pmid", "")).strip()
            if not pmid:
                errors += 1
                continue

            # 已存在则跳过
            if pmid in existing_pmids:
                skipped += 1
                continue

            # 找对应的 MD 文件
            md_path = find_md_file(json_path)
            has_fulltext = 0
            word_count = 0
            md_rel_path = ""

            if md_path and md_path.exists():
                try:
                    with open(md_path, "r", encoding="utf-8") as mf:
                        md_content = mf.read()
                    line_count = md_content.count("\n")
                    has_fulltext = 1 if line_count > 30 else 0
                    word_count = count_words(md_content)
                    try:
                        md_rel_path = str(md_path.relative_to(data_dir.parent))
                    except ValueError:
                        md_rel_path = str(md_path)
                except Exception as e:
                    log_error(conn, pmid, "step1_md_read", str(e))

            # JSON 相对路径
            try:
                json_rel_path = str(json_path.relative_to(data_dir.parent))
            except ValueError:
                json_rel_path = str(json_path)

            # 解析字段
            authors = meta.get("authors", [])
            if isinstance(authors, list):
                authors_str = json.dumps(authors, ensure_ascii=False)
            else:
                authors_str = str(authors)

            year_raw = meta.get("year", None)
            try:
                year = int(year_raw) if year_raw else None
            except (ValueError, TypeError):
                year = None

            row = (
                pmid,
                meta.get("title", ""),
                meta.get("doi", ""),
                meta.get("journal", ""),
                meta.get("volume", ""),
                meta.get("issue", ""),
                year,
                meta.get("month", ""),
                authors_str,
                meta.get("abstract", ""),
                meta.get("matched_keyword", ""),
                meta.get("fulltext_source", ""),
                has_fulltext,
                word_count,
                None,   # impact_factor（Step 3 填写）
                None,   # citation_count（Step 4 填写）
                None,   # study_type（预留）
                None,   # species（预留）
                meta.get("pubmed_url", ""),
                meta.get("doi_url", ""),
                meta.get("download_date", ""),
                md_rel_path,
                json_rel_path,
                indicator_dir,
            )
            batch.append(row)
            existing_pmids.add(pmid)
            inserted += 1

            # 批量提交
            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    """INSERT OR IGNORE INTO papers
                       (pmid, title, doi, journal, volume, issue, year, month, authors,
                        abstract, matched_keyword, fulltext_source, has_fulltext, word_count,
                        impact_factor, citation_count, study_type, species,
                        pubmed_url, doi_url, download_date, md_file_path, json_file_path, indicator_dir)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    batch
                )
                conn.commit()
                batch.clear()

        except Exception as e:
            errors += 1
            try:
                pmid_guess = json_path.name.split("_")[0]
                log_error(conn, pmid_guess, "step1", str(e))
            except Exception:
                pass

    # 提交剩余
    if batch:
        conn.executemany(
            """INSERT OR IGNORE INTO papers
               (pmid, title, doi, journal, volume, issue, year, month, authors,
                abstract, matched_keyword, fulltext_source, has_fulltext, word_count,
                impact_factor, citation_count, study_type, species,
                pubmed_url, doi_url, download_date, md_file_path, json_file_path, indicator_dir)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            batch
        )
        conn.commit()

    print(f"[Step 1] 完成：新增 {inserted} 条，跳过 {skipped} 条，错误 {errors} 条")


# ─────────────────────────────────────────────
# Step 2：免疫指标关键词匹配
# ─────────────────────────────────────────────

def parse_keywords_file(keywords_path: Path) -> List[Tuple[str, List[str]]]:
    """
    解析免疫指标白名单文件。
    返回列表：[(标准名称, [标准名称, 别名1, 别名2, ...]), ...]
    """
    indicators = []
    if not keywords_path.exists():
        print(f"[警告] 关键词文件不存在：{keywords_path}")
        return indicators

    with open(keywords_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("##"):
                continue
            parts = [p.strip() for p in line.split("|")]
            if not parts:
                continue
            standard = parts[0]
            if not standard:
                continue
            aliases = parts  # 标准名称也参与匹配
            indicators.append((standard, aliases))

    print(f"[Step 2] 共解析到 {len(indicators)} 个免疫指标（含别名）")
    return indicators


def build_regex_for_alias(alias: str) -> Optional[re.Pattern]:
    """
    为别名构建词边界正则表达式。
    处理：-、α、β、γ、数字等特殊字符。
    """
    if not alias:
        return None
    escaped = re.escape(alias)
    # re.escape 对 - 会转义为 \-，对 α β γ 等 unicode 不需特殊处理
    # 用 (?<![a-zA-Z0-9]) ... (?![a-zA-Z0-9]) 替代简单 \b，
    # 避免 \b 在非 ASCII 字符边界失效
    pattern = r"(?<![A-Za-z0-9])" + escaped + r"(?![A-Za-z0-9])"
    try:
        return re.compile(pattern, re.IGNORECASE)
    except re.error:
        # 如果正则编译失败，退回简单包含匹配
        try:
            return re.compile(re.escape(alias), re.IGNORECASE)
        except re.error:
            return None


def match_indicators_in_text(
    text: str,
    indicators: List[Tuple[str, List[str]]],
    location: str,
    pmid: str,
) -> List[Tuple[str, str, str, str]]:
    """
    在 text 中匹配所有免疫指标的别名。
    返回 [(pmid, indicator, matched_alias, match_location), ...]，去重。
    """
    if not text:
        return []
    results = []
    seen = set()  # (indicator, location) 去重
    for standard, aliases in indicators:
        for alias in aliases:
            regex = build_regex_for_alias(alias)
            if regex is None:
                continue
            if regex.search(text):
                key = (standard, location)
                if key not in seen:
                    seen.add(key)
                    results.append((pmid, standard, alias, location))
                break  # 同一指标同一位置只记一次（首个命中的别名）
    return results


def match_keywords(conn: sqlite3.Connection, data_dir: Path, keywords_path: Path):
    """
    Step 2：对每篇文献做免疫指标关键词匹配，写入 paper_indicators 表。
    """
    print("\n[Step 2] 开始免疫指标关键词匹配...")

    indicators = parse_keywords_file(keywords_path)
    if not indicators:
        print("[Step 2] 无可用指标，跳过。")
        return

    # 预编译所有正则（避免重复编译）
    compiled: List[Tuple[str, List[Tuple[str, re.Pattern]]]] = []
    for standard, aliases in indicators:
        alias_patterns = []
        for alias in aliases:
            pat = build_regex_for_alias(alias)
            if pat:
                alias_patterns.append((alias, pat))
        if alias_patterns:
            compiled.append((standard, alias_patterns))

    # 获取所有已有 pmid 的匹配记录（避免重复）
    existing_keys = set(
        (row[0], row[1], row[2])
        for row in conn.execute(
            "SELECT pmid, indicator, match_location FROM paper_indicators"
        )
    )

    # 获取所有文献
    papers = conn.execute(
        "SELECT pmid, title, abstract, md_file_path FROM papers"
    ).fetchall()

    total = len(papers)
    print(f"[Step 2] 共 {total} 篇文献需要匹配")

    batch = []
    BATCH_SIZE = 500
    matched_count = 0

    pbar = make_tqdm(papers, total=total, desc="Step 2 关键词匹配", unit="篇")

    for pmid, title, abstract, md_rel_path in pbar:
        try:
            # 读取 MD 全文
            fulltext = ""
            if md_rel_path:
                # md_rel_path 是相对于 data_dir.parent 的路径
                md_abs = data_dir.parent / md_rel_path
                if md_abs.exists():
                    try:
                        with open(md_abs, "r", encoding="utf-8") as f:
                            fulltext = f.read()
                    except Exception:
                        pass

            for standard, alias_patterns in compiled:
                # 在 title、abstract、fulltext 分别匹配
                for location, text in [
                    ("title", title or ""),
                    ("abstract", abstract or ""),
                    ("fulltext", fulltext),
                ]:
                    if not text:
                        continue
                    for alias, pattern in alias_patterns:
                        if pattern.search(text):
                            key = (pmid, standard, location)
                            if key not in existing_keys:
                                existing_keys.add(key)
                                batch.append((pmid, standard, alias, location))
                                matched_count += 1
                            break  # 同一指标同一位置只取首个命中别名

            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    "INSERT INTO paper_indicators (pmid, indicator, matched_alias, match_location) VALUES (?,?,?,?)",
                    batch
                )
                conn.commit()
                batch.clear()

        except Exception as e:
            log_error(conn, pmid, "step2", str(e))

    if batch:
        conn.executemany(
            "INSERT INTO paper_indicators (pmid, indicator, matched_alias, match_location) VALUES (?,?,?,?)",
            batch
        )
        conn.commit()

    print(f"[Step 2] 完成：共写入 {matched_count} 条指标匹配记录")


# ─────────────────────────────────────────────
# Step 3：查询期刊影响因子（OpenAlex API）
# ─────────────────────────────────────────────

OPENALEX_SOURCES_URL = "https://api.openalex.org/sources"
OPENALEX_WORKS_URL = "https://api.openalex.org/works"


def openalex_headers(email: Optional[str] = None) -> dict:
    headers = {"Accept": "application/json"}
    if email:
        headers["mailto"] = email
    return headers


def clean_journal_name(journal_name: str) -> str:
    """
    清洗期刊名，去掉括号内的地名/年份说明，标准化格式。
    例：
      "Journal of immunology (Baltimore, Md. : 1950)" -> "Journal of immunology"
      "Antioxidants (Basel, Switzerland)"             -> "Antioxidants"
      "Proceedings of the National Academy of Sciences of the United States of America"
        -> "Proceedings of the National Academy of Sciences"
    """
    # 去掉括号及其内容（含全角括号）
    name = re.sub(r'\s*[\(\（][^)\）]*[\)\）]', '', journal_name).strip()
    # 去掉末尾冒号/句点/逗号
    name = name.rstrip(':.,').strip()
    # 合并多余空格
    name = re.sub(r'\s+', ' ', name)
    return name


def _parse_openalex_source(item: dict) -> dict:
    """从 OpenAlex sources 返回结果中提取所需字段。"""
    summary = item.get("summary_stats", {}) or {}
    impact_factor = summary.get("2yr_mean_citedness", None)
    concepts = item.get("x_concepts", []) or []
    category = concepts[0].get("display_name", "") if concepts else ""
    issns = item.get("issn", []) or []
    issn = issns[0] if issns else ""
    return {
        "impact_factor": impact_factor,
        "category": category,
        "issn": issn,
        "h_index": None,  # OpenAlex sources 不提供 h_index
    }


def query_journal_info(journal_name: str, email: Optional[str], session: "requests.Session") -> Optional[dict]:
    """
    通过 OpenAlex API 查询期刊信息。
    策略：
      1. 先用原始名查询
      2. 若无结果且原始名含括号/过长，用清洗后的名字重试
    返回 {impact_factor, category, issn, h_index} 或 None。
    """
    def _search(name: str) -> Optional[dict]:
        try:
            params = {"search": name, "per-page": 1}
            if email:
                params["mailto"] = email
            resp = session.get(OPENALEX_SOURCES_URL, params=params, timeout=15)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                return _parse_openalex_source(results[0])
        except Exception:
            pass
        return None

    # 第一次：用原始名
    result = _search(journal_name)
    if result:
        return result

    # 第二次：用清洗后的名字（只有和原始名不同时才重试）
    cleaned = clean_journal_name(journal_name)
    if cleaned and cleaned != journal_name:
        result = _search(cleaned)
        if result:
            return result

    return None


def fetch_journal_impact_factors(conn: sqlite3.Connection, email: Optional[str]):
    """
    Step 3：收集所有期刊名，调用 OpenAlex API 查询影响因子，
    写入 journal_info 表，并更新 papers.impact_factor。

    断点续传策略：
      - 从未查询过的期刊（不在 journal_info 中）→ 直接查询
      - 已查询但 impact_factor IS NULL 的期刊    → 用清洗后的名字重试
    """
    print("\n[Step 3] 开始查询期刊影响因子（OpenAlex API）...")

    if not REQUESTS_AVAILABLE:
        print("[Step 3] requests 库未安装，跳过期刊影响因子查询。运行 pip install requests 后重试。")
        return

    # 收集所有不重复的期刊名
    journals = [
        row[0] for row in conn.execute(
            "SELECT DISTINCT journal FROM papers WHERE journal IS NOT NULL AND journal != ''"
        )
    ]
    # 已查询且 IF 有值的（真正完成的）
    done_journals = set(
        row[0] for row in conn.execute(
            "SELECT journal_name FROM journal_info WHERE impact_factor IS NOT NULL"
        )
    )
    # 已查询但 IF 为 NULL 的（之前匹配失败，可用清洗名重试）
    null_if_journals = set(
        row[0] for row in conn.execute(
            "SELECT journal_name FROM journal_info WHERE impact_factor IS NULL"
        )
    )

    journals_to_query = [j for j in journals if j not in done_journals]
    retry_count = len([j for j in journals_to_query if j in null_if_journals])
    new_count = len(journals_to_query) - retry_count
    print(f"[Step 3] 共 {len(journals)} 个期刊，待查询 {len(journals_to_query)} 个（其中 {new_count} 个新增，{retry_count} 个重试清洗名）")

    session = requests.Session()
    session.headers.update(openalex_headers(email))

    not_found = []
    cleaned_map: Dict[str, str] = {}  # original -> cleaned，记录实际用清洗名查到的期刊
    RATE_LIMIT = 5  # 每秒最多 5 个请求
    interval = 1.0 / RATE_LIMIT

    pbar = make_tqdm(journals_to_query, total=len(journals_to_query), desc="Step 3 期刊查询", unit="个")

    def _flush_batch(b: list):
        if b:
            conn.executemany(
                """INSERT OR REPLACE INTO journal_info
                   (journal_name, impact_factor, category, issn, h_index)
                   VALUES (?,?,?,?,?)""",
                b
            )
            conn.commit()
            b.clear()

    batch = []
    for journal in pbar:
        t0 = time.time()
        info = query_journal_info(journal, email, session)
        if info:
            batch.append((
                journal,
                info["impact_factor"],
                info["category"],
                info["issn"],
                info["h_index"],
            ))
            # 记录是否使用了清洗名（通过比较原名清洗后是否不同且查询成功）
            cleaned = clean_journal_name(journal)
            if cleaned != journal:
                cleaned_map[journal] = cleaned
        else:
            not_found.append(journal)

        # 批量写入
        if len(batch) >= 50:
            _flush_batch(batch)

        # 限速
        elapsed = time.time() - t0
        if elapsed < interval:
            time.sleep(interval - elapsed)

    _flush_batch(batch)

    if cleaned_map:
        print(f"[Step 3] ✅ 通过清洗期刊名额外匹配到 {len(cleaned_map)} 个期刊，例如：")
        for orig, cln in list(cleaned_map.items())[:5]:
            print(f"  「{orig}」→「{cln}」")

    if not_found:
        print(f"[Step 3] {len(not_found)} 个期刊仍未查询到信息：")
        for j in not_found[:20]:
            print(f"  - {j}")
        if len(not_found) > 20:
            print(f"  ... 共 {len(not_found)} 个")

    # 更新 papers.impact_factor（覆盖所有 NULL 记录）
    print("[Step 3] 正在更新 papers 表中的影响因子...")
    conn.execute("""
        UPDATE papers
        SET impact_factor = (
            SELECT ji.impact_factor
            FROM journal_info ji
            WHERE ji.journal_name = papers.journal
              AND ji.impact_factor IS NOT NULL
        )
        WHERE impact_factor IS NULL
    """)
    conn.commit()

    # 导出 journal_info CSV
    csv_path = Path(conn.execute("PRAGMA database_list").fetchone()[2]).parent / "journal_info.csv"
    try:
        import csv
        rows = conn.execute(
            "SELECT journal_name, impact_factor, category, issn, h_index FROM journal_info ORDER BY impact_factor DESC NULLS LAST"
        ).fetchall()
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["期刊名称", "影响因子(2yr)", "学科分类", "ISSN", "H-index"])
            writer.writerows(rows)
        print(f"[Step 3] 期刊信息已导出：{csv_path}")
    except Exception as e:
        print(f"[Step 3] 导出 CSV 失败：{e}")

    print("[Step 3] 期刊影响因子查询完成")


# ─────────────────────────────────────────────
# Step 4：查询论文引用次数（OpenAlex API，断点续传）
# ─────────────────────────────────────────────

def query_citation_count(pmid: str, doi: str, email: Optional[str], session: "requests.Session") -> Optional[int]:
    """
    通过 OpenAlex API 查询单篇论文的引用次数。
    优先按 DOI 查询，失败则按 PMID 查询。
    """
    def _fetch(url: str) -> Optional[int]:
        try:
            params = {}
            if email:
                params["mailto"] = email
            resp = session.get(url, params=params if params else None, timeout=15)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            return data.get("cited_by_count", None)
        except Exception:
            return None

    # 先按 DOI 查
    if doi:
        doi_clean = doi.strip().lstrip("https://doi.org/").lstrip("http://doi.org/")
        if doi_clean:
            count = _fetch(f"{OPENALEX_WORKS_URL}/doi:{doi_clean}")
            if count is not None:
                return count

    # 再按 PMID 查
    if pmid:
        count = _fetch(f"{OPENALEX_WORKS_URL}/pmid:{pmid}")
        if count is not None:
            return count

    return None


def fetch_citation_counts(conn: sqlite3.Connection, email: Optional[str]):
    """
    Step 4：批量查询论文引用次数（断点续传，只查 citation_count IS NULL 的记录）。
    """
    print("\n[Step 4] 开始查询论文引用次数（OpenAlex API，支持断点续传）...")

    if not REQUESTS_AVAILABLE:
        print("[Step 4] requests 库未安装，跳过引用次数查询。运行 pip install requests 后重试。")
        return

    # 只查 citation_count 为 NULL 的
    papers = conn.execute(
        "SELECT pmid, doi FROM papers WHERE citation_count IS NULL"
    ).fetchall()

    total = len(papers)
    print(f"[Step 4] 共 {total} 篇论文待查询引用次数")

    if total == 0:
        print("[Step 4] 无需查询，全部已有引用次数数据")
        return

    session = requests.Session()
    session.headers.update(openalex_headers(email))

    RATE_LIMIT = 5
    interval = 1.0 / RATE_LIMIT
    COMMIT_EVERY = 100

    updated = 0
    not_found = 0
    updates = []

    pbar = make_tqdm(papers, total=total, desc="Step 4 引用次数查询", unit="篇")

    for pmid, doi in pbar:
        t0 = time.time()
        try:
            count = query_citation_count(pmid, doi or "", email, session)
            if count is not None:
                updates.append((count, pmid))
                updated += 1
            else:
                # 标记为 -1 表示查询过但未找到（避免重复查询）
                updates.append((-1, pmid))
                not_found += 1
        except Exception as e:
            log_error(conn, pmid, "step4", str(e))

        if len(updates) >= COMMIT_EVERY:
            conn.executemany(
                "UPDATE papers SET citation_count = ? WHERE pmid = ?",
                updates
            )
            conn.commit()
            updates.clear()

        elapsed = time.time() - t0
        if elapsed < interval:
            time.sleep(interval - elapsed)

    if updates:
        conn.executemany(
            "UPDATE papers SET citation_count = ? WHERE pmid = ?",
            updates
        )
        conn.commit()

    print(f"[Step 4] 完成：查到引用次数 {updated} 篇，未找到 {not_found} 篇")


# ─────────────────────────────────────────────
# Step 5：生成统计摘要
# ─────────────────────────────────────────────

def generate_stats(conn: sqlite3.Connection, db_path: str):
    """
    Step 5：生成统计摘要，输出到控制台和 stats_summary.txt。
    """
    print("\n[Step 5] 生成统计摘要...")

    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def add(text=""):
        lines.append(text)
        print(text)

    add("=" * 60)
    add(f"  免疫学文献知识库 — 统计摘要")
    add(f"  生成时间：{now}")
    add("=" * 60)

    # 总文献数
    total = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    add(f"\n【总文献数】{total:,} 篇")

    # 全文 vs 摘要
    fulltext = conn.execute("SELECT COUNT(*) FROM papers WHERE has_fulltext = 1").fetchone()[0]
    abstract_only = total - fulltext
    add(f"\n【全文 vs 摘要】")
    add(f"  有全文：{fulltext:,} 篇 ({fulltext/total*100:.1f}%)" if total else "  有全文：0 篇")
    add(f"  仅摘要：{abstract_only:,} 篇 ({abstract_only/total*100:.1f}%)" if total else "  仅摘要：0 篇")

    # 免疫指标 Top 20
    add(f"\n【涉及免疫指标 Top 20（按文献数）】")
    rows = conn.execute("""
        SELECT indicator, COUNT(DISTINCT pmid) AS cnt
        FROM paper_indicators
        GROUP BY indicator
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for i, (ind, cnt) in enumerate(rows, 1):
        add(f"  {i:2d}. {ind:<30s} {cnt:,} 篇")

    # 未匹配任何免疫指标的文献数
    unmatched = conn.execute("""
        SELECT COUNT(*) FROM papers
        WHERE pmid NOT IN (SELECT DISTINCT pmid FROM paper_indicators)
    """).fetchone()[0]
    add(f"\n【未匹配到任何免疫指标的文献】{unmatched:,} 篇")

    # 期刊 Top 20
    add(f"\n【涉及期刊 Top 20（按文献数）】")
    rows = conn.execute("""
        SELECT journal, COUNT(*) AS cnt
        FROM papers
        WHERE journal IS NOT NULL AND journal != ''
        GROUP BY journal
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()
    for i, (j, cnt) in enumerate(rows, 1):
        add(f"  {i:2d}. {j:<45s} {cnt:,} 篇")

    # 年份分布
    add(f"\n【年份分布】")
    rows = conn.execute("""
        SELECT year, COUNT(*) AS cnt
        FROM papers
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year DESC
        LIMIT 20
    """).fetchall()
    for year, cnt in rows:
        add(f"  {year}：{cnt:,} 篇")

    # 影响因子分布
    add(f"\n【影响因子分布（基于 OpenAlex 数据）】")
    row = conn.execute("""
        SELECT
            COUNT(*),
            AVG(impact_factor),
            (
                SELECT impact_factor FROM papers
                WHERE impact_factor IS NOT NULL
                ORDER BY impact_factor
                LIMIT 1 OFFSET (
                    SELECT COUNT(*)/2 FROM papers WHERE impact_factor IS NOT NULL
                )
            ),
            MAX(impact_factor)
        FROM papers
        WHERE impact_factor IS NOT NULL
    """).fetchone()
    if row and row[0]:
        add(f"  有影响因子数据：{row[0]:,} 篇")
        add(f"  均值：{row[1]:.2f}")
        add(f"  中位数：{row[2]:.2f}" if row[2] else "  中位数：N/A")
        add(f"  最大值：{row[3]:.2f}")
    else:
        add("  暂无影响因子数据（请先运行 Step 3）")

    # 引用次数分布
    add(f"\n【引用次数分布（基于 OpenAlex 数据）】")
    row = conn.execute("""
        SELECT
            COUNT(*),
            AVG(citation_count),
            MAX(citation_count)
        FROM papers
        WHERE citation_count IS NOT NULL AND citation_count >= 0
    """).fetchone()
    if row and row[0]:
        add(f"  有引用次数数据：{row[0]:,} 篇")
        add(f"  平均被引次数：{row[1]:.1f}")
        add(f"  最多被引次数：{row[2]:,}")
    else:
        add("  暂无引用次数数据（请先运行 Step 4）")

    add("\n" + "=" * 60)

    # 写入文件
    output_dir = Path(db_path).parent
    stats_path = output_dir / "stats_summary.txt"
    with open(stats_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n[Step 5] 统计摘要已保存至：{stats_path}")


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="免疫学文献知识库构建脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  # 运行所有步骤
  python build_knowledge_base.py --data-dir ../downloaded_index --keywords "../immune_keywords全量名单" --db ./immune_kb.db

  # 只运行 Step 1（扫描元数据）
  python build_knowledge_base.py --steps 1

  # 运行 Step 1 和 2
  python build_knowledge_base.py --steps 1,2

  # 查询期刊影响因子（需网络）
  python build_knowledge_base.py --steps 3 --email your@email.com

  # 查询引用次数（断点续传）
  python build_knowledge_base.py --steps 4

  # 生成统计摘要
  python build_knowledge_base.py --steps 5
        """
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="downloaded_index 目录路径（默认：../downloaded_index）"
    )
    parser.add_argument(
        "--keywords",
        default=None,
        help="免疫指标关键词文件路径（默认：自动查找 ../immune_keywords全量名单，回退到 ../immune_keywords.txt）"
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite 数据库文件路径（默认：./immune_kb.db）"
    )
    parser.add_argument(
        "--steps",
        default="1,2,3,4,5",
        help="要执行的步骤，逗号分隔（默认：1,2,3,4,5）"
    )
    parser.add_argument(
        "--email",
        default=None,
        help="OpenAlex API 邮箱（可选，提高速率限制）"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # 解析步骤
    try:
        steps = set(int(s.strip()) for s in args.steps.split(",") if s.strip())
    except ValueError:
        print(f"[错误] --steps 参数格式不正确：{args.steps}（示例：1,2,3）")
        sys.exit(1)

    # 路径解析（相对于脚本所在目录）
    script_dir = Path(__file__).parent
    project_root = script_dir.parent  # build_knowledge_base/ 的上级 = 项目根目录

    data_dir = Path(args.data_dir) if args.data_dir else project_root / "downloaded_index"
    # 优先使用全量名单，回退到 immune_keywords.txt
    if args.keywords:
        keywords_path = Path(args.keywords)
    elif (project_root / "immune_keywords全量名单").exists():
        keywords_path = project_root / "immune_keywords全量名单"
    else:
        keywords_path = project_root / "immune_keywords.txt"
    db_path = args.db if args.db else str(script_dir / "immune_kb.db")

    # 路径校验
    if 1 in steps or 2 in steps:
        if not data_dir.exists():
            print(f"[错误] data-dir 不存在：{data_dir}")
            sys.exit(1)

    print(f"[配置] 数据目录：{data_dir}")
    print(f"[配置] 关键词文件：{keywords_path}")
    print(f"[配置] 数据库路径：{db_path}")
    print(f"[配置] 执行步骤：{sorted(steps)}")
    if args.email:
        print(f"[配置] OpenAlex 邮箱：{args.email}")

    # 初始化数据库
    conn = init_db(db_path)

    try:
        if 1 in steps:
            scan_and_insert_papers(conn, data_dir)

        if 2 in steps:
            if not keywords_path.exists():
                print(f"[Step 2] 关键词文件不存在：{keywords_path}，跳过。")
            else:
                match_keywords(conn, data_dir, keywords_path)

        if 3 in steps:
            if not REQUESTS_AVAILABLE:
                print("[Step 3] 缺少 requests 库，跳过期刊影响因子查询。")
            else:
                fetch_journal_impact_factors(conn, args.email)

        if 4 in steps:
            if not REQUESTS_AVAILABLE:
                print("[Step 4] 缺少 requests 库，跳过引用次数查询。")
            else:
                fetch_citation_counts(conn, args.email)

        if 5 in steps:
            generate_stats(conn, db_path)

    except KeyboardInterrupt:
        print("\n[中断] 用户中断，已提交当前进度，可安全重新运行（支持增量/断点续传）。")
    finally:
        conn.close()
        print("[完成] 数据库已关闭。")


if __name__ == "__main__":
    main()
