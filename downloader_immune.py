#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按免疫指标+期刊名单下载 - 入口脚本
===================================================
下载模式：免疫筛选模式（IMMUNE_FILTER_MODE=True）
期刊名单：../journal_list.txt（根目录）
关键词表：../immune_keywords.txt（根目录）
保存路径：../downloaded_index/{关键词}/{期刊--年份--期次}/

功能说明：
  - 从 PubMed 按期刊名单逐本查询，只保留与免疫关键词相关的文章
  - 文章按得分最高的免疫关键词分组，分别保存在 downloaded_index/{关键词}/ 下
  - 下载策略：PMC XML → PMC PDF → Unpaywall → Sci-Hub → Publisher → Selenium
  - 支持断点续传（进度保存在 downloaded_index/_全局进度.json）
  - 支持多进程并行（通过 run_parallel.sh 启动）

关键词文件格式（immune_keywords.txt）：
  ## 分组名        ← 分组标题（同时作为 downloaded_index/ 的一级目录名）
  IL-6             ← 关键词（支持 PubMed 通配符 *）
  # 这是注释       ← 忽略
  空行              ← 忽略

使用方法：
  单进程:   python3 downloader_immune.py
  后台运行:  ./run_background.sh start
  并行下载:  ./run_parallel.sh [进程数]
  查看进度:  python3 check_progress.py
             或打开 下载报告.md（由 generate_report.py 生成）
"""

import os
import sys
from pathlib import Path

# ── 将根目录加入 Python 路径 ──
ROOT_DIR = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(ROOT_DIR))

from dotenv import load_dotenv

local_env = Path(__file__).parent / ".env"
root_env = ROOT_DIR / ".env"
if local_env.exists():
    load_dotenv(local_env, override=True)
elif root_env.exists():
    load_dotenv(root_env, override=True)

from pubmed_downloader_v4_enhanced import PubMedDownloader


def main():
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║       免疫指标+期刊名单下载器 v4.0                          ║
    ║       模式：免疫筛选模式（按关键词过滤文章）                ║
    ║       保存：downloaded_index/{关键词}/{期刊--年份--期次}/   ║
    ╚══════════════════════════════════════════════════════════════╝
    """)

    # ── 读取配置 ──
    EMAIL    = os.getenv('PUBMED_EMAIL')
    API_KEY  = os.getenv('PUBMED_API_KEY')

    ENABLE_UNPAYWALL = os.getenv('ENABLE_UNPAYWALL', 'True').lower() == 'true'
    ENABLE_SCIHUB    = os.getenv('ENABLE_SCIHUB',    'True').lower() == 'true'
    ENABLE_SELENIUM  = os.getenv('ENABLE_SELENIUM',  'False').lower() == 'true'
    PREFER_XML       = True

    # 关键词文件（根目录）
    IMMUNE_KEYWORDS_FILE = os.getenv('IMMUNE_KEYWORDS_FILE',
                                     str(ROOT_DIR / 'immune_keywords.txt'))

    # 期刊名单路径（根目录）
    JOURNAL_LIST_FILE = str(ROOT_DIR / "journal_list.txt")

    # 并行分片（由 run_parallel.sh 设置）
    JOURNAL_SLICE = os.getenv('JOURNAL_SLICE', '')

    # ── 验证必需配置 ──
    if not EMAIL:
        print("❌ 错误：未设置 PUBMED_EMAIL 环境变量")
        print(f"💡 提示：请编辑 {local_env} 并设置 PUBMED_EMAIL=your_email@example.com")
        return

    if not Path(JOURNAL_LIST_FILE).exists():
        print(f"❌ 错误：找不到期刊名单文件 {JOURNAL_LIST_FILE}")
        return

    if not Path(IMMUNE_KEYWORDS_FILE).exists():
        print(f"❌ 错误：找不到关键词文件 {IMMUNE_KEYWORDS_FILE}")
        print(f"💡 提示：请确认根目录下存在 immune_keywords.txt")
        return

    # ── 显示配置 ──
    print(f"📋 当前配置:")
    print(f"   📧 邮箱:      {EMAIL}")
    print(f"   🔑 API密钥:   {'✅ 已配置' if API_KEY else '❌ 未配置（速率限制 3次/秒）'}")
    print(f"   📄 XML优先:   ✅ 已启用")
    print(f"   🔓 Unpaywall:  {'✅' if ENABLE_UNPAYWALL else '❌'}")
    print(f"   🌐 Sci-Hub:   {'✅ (带年份熔断)' if ENABLE_SCIHUB else '❌'}")
    print(f"   🌐 Selenium:  {'✅' if ENABLE_SELENIUM else '❌'}")
    print(f"   🔬 下载模式:  免疫筛选模式")
    print(f"   📝 关键词文件: {IMMUNE_KEYWORDS_FILE}")
    print(f"   📂 保存路径:  downloaded_index/{{关键词}}/{{期刊--年份--期次}}/")
    print(f"   📋 期刊名单:  {JOURNAL_LIST_FILE}")
    if JOURNAL_SLICE:
        print(f"   🔀 分片范围:  {JOURNAL_SLICE}")
    print()

    # ── 切换工作目录到根目录 ──
    os.chdir(ROOT_DIR)

    # ── 创建下载器（强制免疫筛选模式）──
    downloader = PubMedDownloader(
        email=EMAIL,
        api_key=API_KEY,
        enable_unpaywall=ENABLE_UNPAYWALL,
        enable_scihub=ENABLE_SCIHUB,
        enable_selenium=ENABLE_SELENIUM,
        prefer_xml=PREFER_XML,
        immune_filter_mode=True,               # ← 免疫筛选模式，固定不可更改
        immune_keywords_file=IMMUNE_KEYWORDS_FILE,
    )

    # 打印已加载关键词摘要
    if downloader.immune_keywords:
        groups = {}
        canonical_set = set()
        for item in downloader.immune_keywords:
            alias, grp, canonical = item[0], item[1], item[2] if len(item) >= 3 else item[0]
            canonical_set.add(canonical)
            groups.setdefault(grp, set()).add(canonical)
        print(f"🔬 已加载关键词 ({len(canonical_set)} 个指标，{len(groups)} 个分组):")
        for grp_name, canonical_list in list(groups.items())[:5]:
            preview = "、".join(list(canonical_list)[:4])
            more = f"...共{len(canonical_list)}个" if len(canonical_list) > 4 else ""
            print(f"   [{grp_name}] {preview}{more}")
        if len(groups) > 5:
            print(f"   ... 还有 {len(groups)-5} 个分组（查看 {IMMUNE_KEYWORDS_FILE}）")
        print()

    # ── 开始下载 ──
    try:
        downloader.process_journal_list(JOURNAL_LIST_FILE, journal_slice=JOURNAL_SLICE)
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断下载")
    except Exception as e:
        print(f"\n\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
