#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
免疫文献下载报告生成器 v4
生成按指标分组的 Markdown 下载报告。

目录结构（IMMUNE_FILTER_MODE）：
  downloaded_index/{关键词}/{期刊--年份--期号}/{pmid}_标题.txt  ← 每篇必有（摘要元数据）
  downloaded_index/{关键词}/{期刊--年份--期号}/{pmid}_标题.md   ← 有全文才有（含正文）
  downloaded_index/{关键词}/{期刊--年份--期号}/{pmid}_标题.pdf  ← PDF 全文（可选）

统计逻辑：
  - 总文章数  = .txt 文件数（每篇文章必有一个 .txt，不重复计）
  - 获得全文  = .md 文件数（成功获取正文）
  - PDF 全文  = .pdf 文件数
  - 仅摘要    = 有 .txt 但无对应 .md 的文章数
  - 全文率    = (全文MD + PDF) / 总文章数
"""

import re
import subprocess
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# 月份简写 → 数字，用于排序
MONTH_ORDER = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
    'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'June': 6, 'July': 7, 'August': 8, 'September': 9,
    'October': 10, 'November': 11, 'December': 12,
}

MONTH_CN = {
    1: '1月', 2: '2月', 3: '3月', 4: '4月',
    5: '5月', 6: '6月', 7: '7月', 8: '8月',
    9: '9月', 10: '10月', 11: '11月', 12: '12月',
}


def get_running_pids() -> set:
    """返回当前正在运行的 pubmed_downloader 进程 PID 集合"""
    try:
        result = subprocess.run(
            ['ps', 'aux'], capture_output=True, text=True, timeout=5
        )
        pids = set()
        for line in result.stdout.splitlines():
            if 'pubmed_downloader' in line and 'grep' not in line:
                parts = line.split()
                if len(parts) > 1:
                    try:
                        pids.add(int(parts[1]))
                    except ValueError:
                        pass
        return pids
    except Exception:
        return set()


def parse_worker_log(log_path: Path) -> dict:
    """解析单个 worker 日志，提取最新进度统计。"""
    result = {
        'xml': 0, 'pdf': 0, 'abstract': 0,
        'last_journal': '', 'last_update': '',
    }
    if not log_path.exists():
        return result

    mtime = log_path.stat().st_mtime
    result['last_update'] = datetime.fromtimestamp(mtime).strftime('%H:%M:%S')

    try:
        size = log_path.stat().st_size
        offset = max(0, size - 200 * 1024)
        with open(log_path, 'rb') as f:
            f.seek(offset)
            tail = f.read().decode('utf-8', errors='ignore')
    except Exception:
        return result

    stats_matches = re.findall(
        r'进度统计:\s*XML=(\d+),\s*PDF=(\d+),\s*摘要=(\d+)',
        tail
    )
    if stats_matches:
        last = stats_matches[-1]
        result['xml']      = int(last[0])
        result['pdf']      = int(last[1])
        result['abstract'] = int(last[2])

    journal_matches = re.findall(r'准备处理:\s*(.+)', tail)
    if journal_matches:
        result['last_journal'] = journal_matches[-1].strip()

    return result


def get_realtime_status(project_root: Path, index_dir: Path) -> str:
    """生成实时下载状态 Markdown 区块"""
    # 优先查找当前模块目录的 logs/，其次查根目录的 parallel_logs/
    script_dir = Path(__file__).parent.resolve()
    logs_dir = script_dir / 'logs'
    if not logs_dir.exists() or not list(logs_dir.glob('worker_*.log')):
        logs_dir = project_root / 'parallel_logs'

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    lines = []
    lines.append("## 实时下载状态\n")
    lines.append(f"> 统计时间：{now_str}\n")

    # ── downloaded_index 实时文件统计 ──
    if index_dir.exists():
        txt_count = sum(1 for p in index_dir.rglob('*.txt') if not p.name.startswith('_'))
        md_count  = sum(1 for p in index_dir.rglob('*.md')  if not p.name.startswith('_'))
        pdf_count = sum(1 for p in index_dir.rglob('*.pdf') if not p.name.startswith('_'))
        fulltext_count = md_count
        abstract_only  = txt_count - md_count
        ft_rate_idx = f"{(md_count + pdf_count) / txt_count * 100:.1f}%" if txt_count else "—"
    else:
        txt_count = md_count = pdf_count = 0
        fulltext_count = abstract_only = 0
        ft_rate_idx = "—"

    lines.append("### downloaded_index 文件统计（实时扫描）\n")
    lines.append("| 统计项 | 数量 | 说明 |")
    lines.append("|--------|-----:|------|")
    lines.append(f"| **总文章数** | **{txt_count:,}** | 以 .txt 文件计，每篇文章恰好一个 |")
    lines.append(f"| 获得全文（.md） | {fulltext_count:,} | 含正文章节的 Markdown |")
    lines.append(f"| PDF 全文（.pdf） | {pdf_count:,} | PDF 格式全文 |")
    lines.append(f"| 仅摘要 | {abstract_only:,} | 有 .txt 但无对应 .md |")
    lines.append(f"| **全文率** | **{ft_rate_idx}** | (全文MD + PDF) / 总文章数 |")
    lines.append("")

    # ── Worker 进程状态 ──
    running_pids = get_running_pids()
    is_running   = len(running_pids) > 0
    status_icon  = "🟢 **运行中**" if is_running else "🔴 **已停止**"
    lines.append(f"### 后台进程状态：{status_icon}\n")

    if not logs_dir.exists() or not list(logs_dir.glob('worker_*.log')):
        lines.append("> 未找到 worker 日志文件\n")
        lines.append("---\n")
        return "\n".join(lines)

    log_files = sorted(logs_dir.glob('worker_*.log'))
    lines.append("| Worker | 期刊范围 | XML全文 | PDF | 摘要 | 日志最后更新 | 当前处理期刊 |")
    lines.append("|--------|---------|-------:|----:|-----:|-------------|-------------|")

    total_xml = total_pdf = total_abs = 0
    for log_file in log_files:
        name_match  = re.search(r'worker_\d+_(\d+-\d+)\.log', log_file.name)
        range_label = name_match.group(1) if name_match else log_file.stem
        w = parse_worker_log(log_file)
        total_xml += w['xml']
        total_pdf += w['pdf']
        total_abs += w['abstract']
        journal_short = (w['last_journal'][:30] + '…') if len(w['last_journal']) > 30 else w['last_journal']
        lines.append(
            f"| {log_file.stem} | {range_label} | "
            f"{w['xml']:,} | {w['pdf']:,} | {w['abstract']:,} | "
            f"{w['last_update']} | {journal_short} |"
        )

    grand_total = total_xml + total_pdf + total_abs
    ft_rate_log = f"{(total_xml + total_pdf) / grand_total * 100:.1f}%" if grand_total else "—"
    lines.append(
        f"| **合计** | {len(log_files)} workers | "
        f"**{total_xml:,}** | **{total_pdf:,}** | **{total_abs:,}** | | |"
    )
    lines.append("")
    lines.append(
        f"> 📊 本次会话日志累计：全文 **{total_xml + total_pdf:,}** 篇 · "
        f"摘要 **{total_abs:,}** 篇 · 合计 **{grand_total:,}** 篇 · 全文率 **{ft_rate_log}**\n"
    )
    lines.append("---\n")
    return "\n".join(lines)


def parse_issue_num(issue_str: str) -> int:
    """从 'Issue 3' / 'Vol 314' 等提取数字，用于排序"""
    m = re.search(r'(\d+)', str(issue_str))
    return int(m.group(1)) if m else 9999


def scan_index(index_dir: Path) -> dict:
    """
    扫描 downloaded_index 目录，按文章去重统计。
    结构：downloaded_index/{指标}/{期刊--年份--期号}/{pmid}_标题.txt|md|pdf
    """
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
        'total': 0, 'fulltext': 0, 'pdf': 0
    })))

    for indicator_dir in sorted(index_dir.iterdir()):
        if not indicator_dir.is_dir() or indicator_dir.name.startswith('_'):
            continue
        indicator = indicator_dir.name

        for issue_dir in indicator_dir.iterdir():
            if not issue_dir.is_dir():
                continue

            parts = issue_dir.name.rsplit('--', 2)
            if len(parts) == 3:
                journal_name, year_str, issue_part = parts
            elif len(parts) == 2:
                journal_name, year_str = parts
                issue_part = 'Unknown_Issue'
            else:
                journal_name = issue_dir.name
                year_str = 'Unknown'
                issue_part = 'Unknown_Issue'

            issue_display = issue_part.replace('_', ' ')
            key   = (year_str, issue_display)
            entry = data[indicator][journal_name][key]

            for f in issue_dir.iterdir():
                if f.is_dir() or f.name.startswith('_'):
                    continue
                if f.suffix == '.txt':
                    entry['total'] += 1
                elif f.suffix == '.md':
                    entry['fulltext'] += 1
                elif f.suffix == '.pdf':
                    entry['pdf'] += 1

    return data


def build_report(data: dict, realtime_block: str) -> str:
    now = datetime.now().strftime('%Y-%m-%d %H:%M')

    total_articles = total_ft = total_pdf_all = 0
    summary_rows = []

    for indicator in sorted(data.keys()):
        ind_total = ind_ft = ind_pdf = 0
        for journal in data[indicator].values():
            for entry in journal.values():
                ind_total += entry['total']
                ind_ft    += entry['fulltext']
                ind_pdf   += entry['pdf']
        ind_abstract_only = ind_total - ind_ft - ind_pdf
        total_articles += ind_total
        total_ft       += ind_ft
        total_pdf_all  += ind_pdf
        rate = f"{(ind_ft + ind_pdf) / ind_total * 100:.1f}%" if ind_total else "—"
        summary_rows.append((indicator, ind_total, ind_ft, ind_pdf, ind_abstract_only, rate))

    grand_abstract_only = total_articles - total_ft - total_pdf_all
    num_indicators = len(data)

    lines = []
    lines.append("# 免疫文献下载报告\n")
    lines.append(
        f"> 生成时间：{now}　|　指标数：{num_indicators}　|　"
        f"总文章：**{total_articles:,}** 篇　|　"
        f"全文（MD）：{total_ft:,}　|　PDF：{total_pdf_all:,}　|　仅摘要：{grand_abstract_only:,}\n"
    )

    lines.append(realtime_block)

    lines.append("## 各指标概况\n")
    lines.append("| 指标 | 总文章 | 全文MD | PDF | 仅摘要 | 全文率 |")
    lines.append("|------|-------:|------:|----:|-------:|-------:|")
    for indicator, ind_total, ind_ft, ind_pdf, ind_abs_only, rate in summary_rows:
        lines.append(f"| {indicator} | {ind_total:,} | {ind_ft:,} | {ind_pdf:,} | {ind_abs_only:,} | {rate} |")

    lines.append("")
    lines.append("---\n")

    for indicator in sorted(data.keys()):
        journals = data[indicator]
        ind_total = sum(e['total']    for j in journals.values() for e in j.values())
        ind_ft    = sum(e['fulltext'] for j in journals.values() for e in j.values())
        ind_pdf   = sum(e['pdf']      for j in journals.values() for e in j.values())
        ind_abs   = ind_total - ind_ft - ind_pdf

        lines.append(
            f"## {indicator}（共 {ind_total:,} 篇 · 全文MD {ind_ft:,} · PDF {ind_pdf:,} · 仅摘要 {ind_abs:,}）\n"
        )

        for journal_name in sorted(journals.keys()):
            issues  = journals[journal_name]
            j_total = sum(e['total']    for e in issues.values())
            j_ft    = sum(e['fulltext'] for e in issues.values())
            j_pdf   = sum(e['pdf']      for e in issues.values())
            j_abs   = j_total - j_ft - j_pdf

            lines.append(f"### {journal_name}（共 {j_total:,} 篇）\n")
            lines.append("| 期号 | 年份 | 总文章 | 全文MD | PDF | 仅摘要 |")
            lines.append("|------|------|------:|------:|----:|-------:|")

            sorted_issues = sorted(
                issues.items(),
                key=lambda kv: (kv[0][0], parse_issue_num(kv[0][1]))
            )
            for (year_str, issue_display), entry in sorted_issues:
                abs_only = entry['total'] - entry['fulltext'] - entry['pdf']
                lines.append(
                    f"| {issue_display} | {year_str} | "
                    f"{entry['total']} | {entry['fulltext']} | {entry['pdf']} | {abs_only} |"
                )
            lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="生成免疫文献下载 Markdown 报告（含实时进程状态）"
    )
    parser.add_argument('--index-dir', default=None, help='downloaded_index 目录路径')
    parser.add_argument('--output',    default=None, help='输出 .md 文件路径')
    args = parser.parse_args()

    script_dir   = Path(__file__).parent.resolve()
    project_root = script_dir.parent  # 根目录
    index_dir    = Path(args.index_dir).resolve() if args.index_dir else project_root / 'downloaded_index'
    output_path  = Path(args.output).resolve()    if args.output    else script_dir / '下载报告.md'

    if not index_dir.is_dir():
        print(f"[错误] 找不到 downloaded_index 目录：{index_dir}")
        return

    print(f"[信息] 扫描目录：{index_dir}")
    print("[信息] 正在统计文件，请稍候…")
    data = scan_index(index_dir)
    print(f"[信息] 共发现 {len(data)} 个指标")

    print("[信息] 收集实时下载状态…")
    realtime_block = get_realtime_status(project_root, index_dir)

    print("[信息] 生成报告…")
    report = build_report(data, realtime_block)
    output_path.write_text(report, encoding='utf-8')
    print(f"[完成] 报告已保存到：{output_path}")


if __name__ == '__main__':
    main()
