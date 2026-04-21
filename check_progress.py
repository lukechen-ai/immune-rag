#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
免疫指标下载 - 快速进度查询
扫描 downloaded_index/ 目录，统计各指标的文章数量。
"""

import sys
import json
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(ROOT_DIR))

INDEX_DIR = ROOT_DIR / "downloaded_index"


def check_progress():
    if not INDEX_DIR.exists():
        print("❌ downloaded_index/ 目录不存在，尚未开始下载")
        return

    # 读取全局进度 JSON
    stats_file = INDEX_DIR / "_全局进度.json"
    stats = {}
    if stats_file.exists():
        with open(stats_file, encoding='utf-8') as f:
            stats = json.load(f)

    completed = len(stats.get('completed_journal_years', []))
    total_articles = stats.get('total_articles', 0)
    pmc_xml = stats.get('pmc_xml', 0)
    pmc_pdf = stats.get('pmc_pdf', 0)
    unpaywall = stats.get('unpaywall_pdf', 0)
    scihub = stats.get('scihub_pdf', 0)
    abstract_only = stats.get('abstract_only', 0)
    last_update = stats.get('last_update', '未知')

    # 统计各指标目录
    kw_dirs = [d for d in INDEX_DIR.iterdir() if d.is_dir() and not d.name.startswith('_')]

    kw_stats = []
    total_txt = 0
    total_md = 0
    total_pdf = 0
    for kw_dir in sorted(kw_dirs):
        txt = list(kw_dir.rglob("*.txt"))
        md = list(kw_dir.rglob("*.md"))
        pdf = list(kw_dir.rglob("*.pdf"))
        kw_stats.append((kw_dir.name, len(txt), len(md), len(pdf)))
        total_txt += len(txt)
        total_md += len(md)
        total_pdf += len(pdf)

    print(f"\n{'='*65}")
    print(f"  免疫指标+期刊下载进度")
    print(f"  数据目录: {INDEX_DIR}")
    print(f"{'='*65}")
    print(f"  📂 关键词目录数:    {len(kw_dirs)}")
    print(f"  📄 文章总数 (txt):  {total_txt:,}")
    print(f"  📝 有全文 (md/xml): {total_md:,}")
    print(f"  📕 PDF 全文:        {total_pdf:,}")
    if total_txt > 0:
        fulltext_rate = (total_md + total_pdf) * 100 // total_txt
        print(f"  ✅ 全文获取率:      {fulltext_rate}%")

    if stats:
        print(f"{'='*65}")
        print(f"  📊 已完成期刊年份:  {completed}")
        print(f"  📋 已处理文章总数:  {total_articles:,}")
        print(f"  📄 PMC XML:         {pmc_xml:,}")
        print(f"  📕 PDF (全渠道):    {pmc_pdf + unpaywall + scihub:,}")
        print(f"  📋 仅摘要:          {abstract_only:,}")
        print(f"  🕐 最后更新:        {last_update[:19] if last_update != '未知' else '未知'}")

    # 打印 Top10 关键词
    print(f"{'='*65}")
    print(f"  {'关键词':<25} {'文章数':>6}  {'全文':>6}  {'PDF':>5}")
    print(f"  {'-'*50}")
    for kw, txt, md, pdf in sorted(kw_stats, key=lambda x: -x[1])[:15]:
        print(f"  {kw:<25} {txt:>6}  {md:>6}  {pdf:>5}")
    if len(kw_stats) > 15:
        print(f"  ... 还有 {len(kw_stats)-15} 个关键词目录")
    print(f"{'='*65}")
    print(f"\n💡 生成详细 Markdown 报告：")
    print(f"   python3 generate_report.py\n")


if __name__ == "__main__":
    check_progress()
