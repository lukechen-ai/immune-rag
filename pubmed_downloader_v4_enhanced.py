#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PubMed期刊论文批量下载器 v4.0 增强版（安全优化版）
- 5级智能下载策略：PMC XML优先 → PMC PDF → Unpaywall → Sci-Hub → Publisher → Selenium
- 集成Unpaywall API（合法开放获取论文）
- 集成undetected-chromedriver（绕过机器人检测）
- XML优先策略（AI训练优化）
- 元数据自动保存（JSON格式）
- 年份熔断机制（跳过Sci-Hub无效请求）
- PDF完整性校验（防止坏死文件）
- 环境变量管理（API密钥安全）
"""

import os
import time
import re
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from Bio import Entrez
from xml.etree import ElementTree as ET
import hashlib
import ssl
import urllib3
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import faulthandler
import signal
import socket
from urllib.request import urlopen as _stdlib_urlopen

# 彻底禁用SSL警告和验证
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl._create_default_https_context = ssl._create_unverified_context

# 加载环境变量
load_dotenv()

# 允许在进程卡住时手动触发线程堆栈打印：
#   kill -USR1 <PID>  → 堆栈会写入当前日志（stderr/stdout）
faulthandler.enable()
try:
    faulthandler.register(signal.SIGUSR1, all_threads=True)
except Exception:
    pass

# 为 Biopython Entrez（urllib）设置全局 socket 超时，避免偶发网络抖动导致永久阻塞
socket.setdefaulttimeout(20)


class PubMedDownloader:
    """PubMed论文下载器 v4.0 增强版（安全优化版）"""
    
    # Sci-Hub镜像站列表（2025年1月更新）
    SCIHUB_MIRRORS = [
        "https://sci-hub.st",
        "https://sci-hub.ru",
        "https://sci-hub.se",
        "https://sci-hub.ee",
        "https://sci-hub.ren",
        "https://sci-hub.hkvisa.net",
    ]
    
    # Sci-Hub年份限制（2022年后基本无新文章）
    # 设置为None表示不进行年份熔断
    SCIHUB_CUTOFF_YEAR = 2023  # 启用年份熔断（推荐）
    
    # PDF最小文件大小（KB），小于此值视为无效文件
    MIN_PDF_SIZE_KB = 10
    
    def __init__(self, 
                 email: str, 
                 api_key: str = None, 
                 enable_unpaywall: bool = True,
                 enable_scihub: bool = True,
                 enable_selenium: bool = False,
                 prefer_xml: bool = True,
                 immune_filter_mode: bool = False,
                 immune_keywords: list = None,
                 immune_keywords_file: str = "immune_keywords.txt"):
        """
        初始化下载器
        
        Args:
            email: 你的邮箱地址（PubMed和Unpaywall要求）
            api_key: PubMed API密钥（可选）
            enable_unpaywall: 是否启用Unpaywall（推荐，合法OA）
            enable_scihub: 是否启用Sci-Hub下载
            enable_selenium: 是否启用Selenium（速度慢，最后备选）
            prefer_xml: 是否优先下载XML（推荐用于AI训练）
            immune_filter_mode: 是否启用免疫筛选模式（指标+白名单期刊，默认关=全量下载）
            immune_keywords: 关键词列表 [(keyword, group), ...]（为None时从文件自动加载）
            immune_keywords_file: 关键词白名单文件路径（默认 immune_keywords.txt）
        """
        Entrez.email = email
        self.email = email  # 保存email供Unpaywall使用
        if api_key:
            Entrez.api_key = api_key

        # NCBI Entrez（Biopython）底层使用 urllib.request.urlopen，默认无超时，可能永久阻塞。
        # 这里替换 Bio.Entrez 模块内的 urlopen 全局函数，强制加 timeout。
        try:
            Entrez.urlopen = lambda request: _stdlib_urlopen(request, timeout=20)
        except Exception:
            pass
        
        # 免疫筛选模式配置
        self.immune_filter_mode = immune_filter_mode
        if immune_filter_mode:
            if immune_keywords is not None:
                self.immune_keywords = immune_keywords
            else:
                self.immune_keywords = self.load_immune_keywords(immune_keywords_file)
            if not self.immune_keywords:
                print(f"⚠️  警告：免疫筛选模式已启用，但关键词列表为空（文件：{immune_keywords_file}）")
        else:
            self.immune_keywords = immune_keywords or []

        # 预编译关键词匹配器（显著加速 find_best_keyword：避免对每篇文章×每个关键词重复编译正则）
        # 兼容 PubMed 通配符 *：转为 \w*
        # 新格式：每个元组为 (kind, alias_lower, compiled_pattern, canonical_name)
        # canonical_name 即标准名称，用于 downloaded_index/ 目录名
        self._immune_matchers = []
        for item in (self.immune_keywords or []):
            kw = item[0]      # alias（别名）
            canonical = item[2] if len(item) >= 3 else kw  # 标准名称
            if "*" in kw:
                pattern = re.compile(re.escape(kw).replace(r"\*", r"\w*"), re.IGNORECASE)
                self._immune_matchers.append(("regex", kw, pattern, canonical))
            else:
                self._immune_matchers.append(("literal", kw.lower(), None, canonical))
        
        # 设置下载根目录（根据模式分叉）
        if immune_filter_mode:
            self.download_dir = Path("downloaded_index")
        else:
            self.download_dir = Path("downloaded_papers")
        self.download_dir.mkdir(exist_ok=True)
        
        # 请求延迟（秒）
        self.delay = 0.34 if not api_key else 0.1
        
        # XML优先策略
        self.prefer_xml = prefer_xml
        
        # Unpaywall配置
        self.enable_unpaywall = enable_unpaywall
        
        # Sci-Hub配置
        self.enable_scihub = enable_scihub
        self.current_scihub_mirror = 0
        
        # Selenium配置
        self.selenium_enabled = enable_selenium
        
        # 当前处理年份（用于熔断机制）
        self.current_year = None
        
        # 统计数据
        self.stats = {
            'total': 0,
            'pmc_xml': 0,          # PMC XML（AI训练最佳）
            'pmc_pdf': 0,
            'unpaywall_pdf': 0,
            'scihub_pdf': 0,
            'scihub_skipped': 0,   # 因年份熔断跳过的
            'publisher_pdf': 0,
            'selenium_pdf': 0,
            'xml_converted': 0,
            'abstract_only': 0,
            'failed': 0,
            'corrupted_removed': 0,  # 移除的损坏文件数
            'dedup_skipped': 0       # 因 PMID 跨期刊名去重跳过的
        }
        
        # 失败文章清单
        self.failed_articles = []
        self._stats_lock = threading.Lock()        # 保护 self.stats 并发写
        self._pmids_lock = threading.Lock()         # 保护 downloaded_pmids 并发写
        self._failed_lock = threading.Lock()        # 保护 failed_articles 并发写

        # 全局下载 PMID 去重集合（跨期刊名，防止全名/缩写重复下载）
        self.downloaded_pmids_file = self.download_dir / "_downloaded_pmids.txt"
        self.downloaded_pmids: set = self._load_downloaded_pmids()
        
        # 全局统计文件路径
        self.global_stats_file = self.download_dir / "_全局进度.json"
        self.global_report_file = self.download_dir / "_全局下载报告.txt"
        self.global_stats_lock = threading.Lock()
        
        # 加载或初始化全局统计
        self.global_stats = self._load_global_stats()
        
        # 设置请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        # 创建自定义session
        self.session = requests.Session()
        self.session.verify = False
        
        # 添加重试策略（增强版：包含网络连接错误）
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # ── NCBI API 速率限制器 ──────────────────────────────────
        # 有 API Key：最多 10次/秒；无 Key：最多 3次/秒
        # 用令牌桶算法：每次 Entrez 调用前必须 acquire()
        self._rate_limit = 10 if api_key else 3
        self._rate_semaphore = threading.Semaphore(self._rate_limit)
        self._rate_lock = threading.Lock()
        self._rate_window_start = time.monotonic()
        self._rate_call_count = 0

    def _stat_inc(self, key: str, delta: int = 1):
        """线程安全地对 stats 字典中的计数器加 delta"""
        with self._stats_lock:
            self.stats[key] += delta

    def _ncbi_acquire(self):
        """令牌桶：确保每秒调用 NCBI API 不超过速率上限（线程安全）"""
        with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._rate_window_start
            if elapsed >= 1.0:
                # 新的一秒窗口：重置计数
                self._rate_window_start = now
                self._rate_call_count = 0
            if self._rate_call_count >= self._rate_limit:
                # 当前窗口已满，等到下一秒
                sleep_time = 1.0 - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)
                self._rate_window_start = time.monotonic()
                self._rate_call_count = 0
            self._rate_call_count += 1

    def check_network_health(self, max_attempts: int = 3) -> bool:
        """
        检查网络健康状态（ping PubMed）
        
        Args:
            max_attempts: 最大尝试次数
            
        Returns:
            bool: 网络是否正常
        """
        test_urls = [
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/",
            "https://www.ncbi.nlm.nih.gov/",
            "https://www.google.com"
        ]
        
        for attempt in range(max_attempts):
            for url in test_urls:
                try:
                    response = self.session.get(url, timeout=5)
                    if response.status_code < 500:
                        return True
                except:
                    continue
            
            if attempt < max_attempts - 1:
                wait_time = (attempt + 1) * 5
                print(f"      ⚠️  网络连接失败，{wait_time}秒后重试...")
                time.sleep(wait_time)
        
        return False
    
    def wait_for_network_recovery(self, check_interval: int = 30):
        """
        等待网络恢复（断网时暂停任务）
        
        Args:
            check_interval: 检查间隔（秒）
        """
        print(f"\n{'='*80}")
        print(f"🌐 检测到网络故障，任务已暂停")
        print(f"⏱️  每{check_interval}秒自动检测一次，网络恢复后将继续下载")
        print(f"{'='*80}\n")
        
        attempt = 0
        while True:
            attempt += 1
            print(f"[尝试 {attempt}] 检测网络连接...", end=" ", flush=True)
            
            if self.check_network_health(max_attempts=1):
                print(f"✅ 网络已恢复！")
                print(f"🚀 继续下载任务...\n")
                return True
            
            print(f"❌ 仍无法连接")
            time.sleep(check_interval)
        
    @staticmethod
    def load_immune_keywords(filepath: str = "immune_keywords.txt") -> list:
        """
        从文件加载免疫关键词白名单（支持别名映射格式 v2）

        文件格式：
            ## 分组名             → 更新当前分组名
            标准名称              → 单一名称（旧格式，向后兼容）
            标准名称 | 别名1 | 别名2 | ...
                                  → | 左侧第一个为标准名称（用于目录名）
                                    右侧为同一指标的别名（均用于 PubMed 查询）
            # 注释               → 忽略
            空行                  → 忽略

        Returns:
            list: [(alias, group, canonical_name), ...]
                  alias         — 用于 PubMed 查询的关键词（含别名）
                  group         — 所属分组
                  canonical_name — 标准名称（用于 downloaded_index/ 目录名）
        """
        keywords = []
        current_group = "general"

        if not os.path.exists(filepath):
            print(f"⚠️  关键词文件不存在: {filepath}")
            return keywords

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith('##'):
                        current_group = line[2:].strip()
                        continue
                    if line.startswith('#'):
                        continue
                    # 解析别名映射：标准名称 | 别名1 | 别名2 | ...
                    parts = [p.strip() for p in line.split('|') if p.strip()]
                    if not parts:
                        continue
                    canonical = parts[0]   # 第一个为标准名称
                    for alias in parts:    # 所有词（含标准名称本身）都参与查询
                        keywords.append((alias, current_group, canonical))

            print(f"✅ 已加载关键词文件: {filepath}")
            # 统计指标数（按标准名称去重）
            canonicals = set(c for _, _, c in keywords)
            groups = set(g for _, g, _ in keywords)
            print(f"   📝 共 {len(canonicals)} 个指标，{len(keywords)} 个查询词，{len(groups)} 个分组")
        except Exception as e:
            print(f"❌ 加载关键词文件失败: {filepath}: {e}")

        return keywords
    
    def build_pubmed_keyword_query(self) -> str:
        """
        将关键词列表构建为 PubMed OR 查询字符串（使用全部关键词，不截断）
        
        Returns:
            str: PubMed 查询串，例如 'IL-6[tiab] OR interleukin-6[tiab] OR TNF-alpha[tiab]'
        """
        if not self.immune_keywords:
            return ""
        
        parts = []
        seen = set()  # 去重，避免同一别名出现多次
        for item in self.immune_keywords:
            kw = item[0]  # alias
            if kw in seen:
                continue
            seen.add(kw)
            # 含空格的关键词加引号，否则直接加[tiab]
            if ' ' in kw:
                parts.append(f'"{kw}"[tiab]')
            else:
                parts.append(f'{kw}[tiab]')
        
        return " OR ".join(parts)
    
    def find_best_keyword(self, article: Dict) -> str:
        """
        找出文章得分最高的关键词，返回其「标准名称」（用于 downloaded_index/ 目录名）

        匹配逻辑：在文章标题+摘要中统计各别名命中次数，取命中次数最高的别名，
        然后返回该别名对应的「标准名称」（canonical），确保同一指标的不同别名
        命中后都保存到同一个目录下。

        Args:
            article: 文章信息字典（含 title、abstract 字段）

        Returns:
            str: 得分最高的别名所对应的标准名称（已做文件名安全处理）
        """
        if not self.immune_keywords:
            return "general"

        title = str(article.get('title', ''))
        abstract = str(article.get('abstract', ''))
        text_lower = f"{title} {abstract}".lower()

        best_canonical = self.immune_keywords[0][2] if len(self.immune_keywords[0]) >= 3 else self.immune_keywords[0][0]
        best_score = 0

        for kind, keyword, compiled, canonical in self._immune_matchers:
            if kind == "literal":
                score = text_lower.count(keyword)
            else:
                try:
                    score = len(compiled.findall(text_lower))
                except Exception:
                    continue
            if score > best_score:
                best_score = score
                best_canonical = canonical

        # 对标准名称做文件名安全处理（* 替换为 _star）
        safe_kw = best_canonical.replace('*', '_star')
        safe_kw = re.sub(r'[<>:"/\\|?]', '_', safe_kw).strip('. ')
        return safe_kw or "general"
    
    def sanitize_filename(self, name: str) -> str:
        """清理文件/文件夹名称"""
        name = re.sub(r'[<>:"/\\|?*]', '_', name)
        name = name.strip('. ')
        return name[:200]
    
    def validate_pdf(self, pdf_path: Path) -> bool:
        """
        验证PDF文件完整性
        
        Args:
            pdf_path: PDF文件路径
            
        Returns:
            bool: 文件是否有效
        """
        try:
            if not pdf_path.exists():
                return False
            
            file_size_kb = pdf_path.stat().st_size / 1024
            
            # 检查1: 文件大小
            if file_size_kb < self.MIN_PDF_SIZE_KB:
                print(f"      ⚠️  PDF文件过小 ({file_size_kb:.1f}KB < {self.MIN_PDF_SIZE_KB}KB)，视为无效")
                pdf_path.unlink()  # 删除无效文件
                self._stat_inc('corrupted_removed')
                return False
            
            # 检查2: PDF文件头
            with open(pdf_path, 'rb') as f:
                header = f.read(4)
                if not header.startswith(b'%PDF'):
                    print(f"      ⚠️  文件头无效，不是合法PDF")
                    pdf_path.unlink()
                    self._stat_inc('corrupted_removed')
                    return False
            
            return True
            
        except Exception as e:
            print(f"      ⚠️  PDF验证失败: {str(e)}")
            if pdf_path.exists():
                pdf_path.unlink()
                self._stat_inc('corrupted_removed')
            return False
    
    def save_metadata(self, article: Dict, output_dir: Path, base_filename: str, source: str):
        """
        保存文章元数据为JSON（用于RAG检索）
        
        Args:
            article: 文章信息字典
            output_dir: 输出目录
            base_filename: 基础文件名
            source: 全文来源（PMC_XML, PMC_PDF, SCIHUB_PDF等）
        """
        try:
            metadata = {
                'pmid': article['pmid'],
                'title': article['title'],
                'doi': article.get('doi'),
                'journal': article['journal'],
                'volume': article.get('volume', ''),
                'issue': article.get('issue', ''),
                'year': article.get('year', ''),
                'month': article.get('month', ''),
                'authors': article.get('authors', []),
                'abstract': article.get('abstract', ''),
                'fulltext_source': source,
                'download_mode': 'immune_filter' if self.immune_filter_mode else 'full',
                'matched_keyword': article.get('_matched_keyword', ''),
                'download_date': datetime.now().isoformat(),
                'pubmed_url': f"https://pubmed.ncbi.nlm.nih.gov/{article['pmid']}/",
                'doi_url': f"https://doi.org/{article['doi']}" if article.get('doi') else None
            }
            
            json_file = output_dir / f"{base_filename}_metadata.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            print(f"      ⚠️  元数据保存失败: {str(e)}")
    
    def get_doi_from_article(self, article_data: Dict) -> Optional[str]:
        """从文章数据中提取DOI"""
        try:
            if 'ELocationID' in article_data:
                for eloc in article_data['ELocationID']:
                    if eloc.attributes.get('EIdType') == 'doi':
                        return str(eloc)
            
            if 'ArticleIdList' in article_data:
                for article_id in article_data['ArticleIdList']:
                    if article_id.attributes.get('IdType') == 'doi':
                        return str(article_id)
            
            return None
        except:
            return None
    
    
    def download_pmc_xml(self, pmc_id: str, xml_path: Path, md_path: Path) -> bool:
        """
        从PMC下载XML格式全文（AI训练最佳格式）
        
        优势：
        - 结构化数据（<abstract>, <body>, <ref>标签清晰）
        - 无排版噪音（相比PDF）
        - 解析错误率为0
        - 文件体积小
        
        Args:
            pmc_id: PMC ID
            xml_path: XML输出路径
            md_path: Markdown输出路径
            
        Returns:
            bool: 是否成功
        """
        try:
            print(f"      📄 尝试从PMC下载XML（AI训练最佳格式）...")
            
            self._ncbi_acquire()
            handle = Entrez.efetch(
                db="pmc",
                id=pmc_id,
                rettype="xml",
                retmode="xml"
            )
            
            xml_content = handle.read()
            handle.close()
            
            # 保存原始XML
            with open(xml_path, 'wb') as f:
                f.write(xml_content)
            
            # 转换为Markdown
            if self.convert_xml_to_markdown(xml_path, md_path):
                print(f"      ✅ PMC XML下载并转换成功")
                self._stat_inc('pmc_xml')
                return True
            else:
                # 转换失败但XML有效，仍视为成功
                print(f"      ✅ PMC XML下载成功（Markdown转换失败）")
                self._stat_inc('pmc_xml')
                return True
                
        except Exception as e:
            print(f"      ⚠️  PMC XML下载失败: {str(e)}")
            if xml_path.exists():
                xml_path.unlink()
            return False
    
    def download_pmc_pdf(self, pmc_id: str, output_path: Path) -> bool:
        """从PMC下载PDF格式全文"""
        pdf_urls = [
            f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/pdf/",
            f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/pdf",
            f"https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_pdf/{pmc_id[:2]}/{pmc_id[:4]}/PMC{pmc_id}.pdf",
        ]
        
        print(f"      🔗 尝试从PMC下载PDF...")
        
        for pdf_url in pdf_urls:
            try:
                for retry in range(2):
                    try:
                        response = self.session.get(
                            pdf_url, 
                            headers=self.headers, 
                            timeout=30, 
                            allow_redirects=True
                        )
                        
                        if response.status_code == 200:
                            content_type = response.headers.get('Content-Type', '')
                            
                            if 'application/pdf' in content_type or response.content.startswith(b'%PDF'):
                                with open(output_path, 'wb') as f:
                                    f.write(response.content)
                                
                                # 验证PDF完整性
                                if self.validate_pdf(output_path):
                                    print(f"      ✅ PMC PDF下载成功 ({output_path.stat().st_size // 1024}KB)")
                                    self._stat_inc('pmc_pdf')
                                    return True
                        
                        break
                        
                    except requests.exceptions.SSLError:
                        if retry == 0:
                            time.sleep(1)
                        else:
                            break
                    except requests.exceptions.ConnectionError:
                        break
                        
            except Exception:
                continue
        
        return False
    
    def download_from_unpaywall(self, doi: str, output_path: Path) -> bool:
        """
        通过Unpaywall API下载合法开放获取论文PDF
        
        Unpaywall优势：
        - ✅ 完全合法（只索引合法OA论文）
        - 🌐 覆盖广（3000万+篇免费论文）
        - ⚡ 速度快（API响应快，无需抓取）
        - 🆓 免费（只需提供邮箱）
        
        Args:
            doi: 文章DOI
            output_path: 输出路径
            
        Returns:
            bool: 是否成功
        """
        if not self.enable_unpaywall or not doi:
            return False
        
        try:
            print(f"      🔓 尝试Unpaywall开放获取下载...")
            
            # Unpaywall API endpoint
            api_url = f"https://api.unpaywall.org/v2/{doi}"
            params = {'email': self.email}
            
            response = self.session.get(
                api_url, 
                params=params,
                headers=self.headers, 
                timeout=10
            )
            
            if response.status_code != 200:
                if response.status_code == 404:
                    print(f"      ℹ️  Unpaywall未收录此DOI")
                else:
                    print(f"      ⚠️  Unpaywall API返回状态码: {response.status_code}")
                return False
            
            data = response.json()
            
            # 检查是否有OA版本
            if not data.get('is_oa', False):
                print(f"      ℹ️  此文章不是开放获取")
                return False
            
            # 获取最佳OA位置
            best_oa = data.get('best_oa_location')
            if not best_oa:
                print(f"      ⚠️  未找到OA PDF链接")
                return False
            
            pdf_url = best_oa.get('url_for_pdf') or best_oa.get('url')
            if not pdf_url:
                print(f"      ⚠️  未找到有效的PDF URL")
                return False
            
            # 显示OA来源信息
            oa_source = best_oa.get('host_type', 'unknown')
            version = best_oa.get('version', 'unknown')
            print(f"      📚 找到OA来源: {oa_source} (版本: {version})")
            print(f"      📄 PDF链接: {pdf_url[:80]}...")
            
            # 🔧 增强的请求头（模拟真实浏览器）
            enhanced_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': f'https://doi.org/{doi}',  # 重要：告诉服务器我们从DOI页面来
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'cross-site',
                'Cache-Control': 'max-age=0',
            }
            
            # 🔁 尝试多种下载策略
            pdf_response = None
            
            # 策略1: 使用增强请求头直接下载
            try:
                print(f"      🔄 策略1: 直接下载...", flush=True)
                pdf_response = self.session.get(
                    pdf_url,
                    headers=enhanced_headers,
                    timeout=15,  # 🔥 缩短超时: 30秒 → 15秒
                    allow_redirects=True
                )
                
                if pdf_response.status_code == 200 and pdf_response.content.startswith(b'%PDF'):
                    print(f"      ✅ 策略1成功")
                else:
                    print(f"      ⚠️  策略1失败 (状态码: {pdf_response.status_code})")
                    pdf_response = None
            except Exception as e:
                print(f"      ⚠️  策略1失败: {str(e)[:50]}")
                pdf_response = None
            
            # 策略2: 如果是publisher类型，尝试通过DOI重定向
            if not pdf_response and oa_source == 'publisher':
                try:
                    print(f"      🔄 策略2: 通过DOI重定向...", flush=True)
                    time.sleep(0.5)  # 🔥 缩短延迟: 1秒 → 0.5秒
                    
                    # 先访问DOI页面（建立合法会话）
                    doi_url = f"https://doi.org/{doi}"
                    doi_response = self.session.get(
                        doi_url,
                        headers=enhanced_headers,
                        timeout=10,  # 🔥 缩短超时: 15秒 → 10秒
                        allow_redirects=True
                    )
                    
                    if doi_response.status_code == 200:
                        # 从重定向后的页面获取cookie，再下载PDF
                        final_url = doi_response.url
                        enhanced_headers['Referer'] = final_url
                        
                        pdf_response = self.session.get(
                            pdf_url,
                            headers=enhanced_headers,
                            timeout=15,  # 🔥 缩短超时: 30秒 → 15秒
                            allow_redirects=True
                        )
                        
                        if pdf_response.status_code == 200 and pdf_response.content.startswith(b'%PDF'):
                            print(f"      ✅ 策略2成功")
                        else:
                            print(f"      ⚠️  策略2失败 (状态码: {pdf_response.status_code})")
                            pdf_response = None
                except Exception as e:
                    print(f"      ⚠️  策略2失败: {str(e)[:50]}")
                    pdf_response = None
            
            # 🔥 优化：跳过策略3（成功率低，耗时长）
            # 策略3: 尝试所有可用的OA位置（不只是best_oa_location）
            # 已禁用以提高速度，如需启用请取消注释
            # if not pdf_response:
            #     oa_locations = data.get('oa_locations', [])
            #     if len(oa_locations) > 1:
            #         print(f"      🔄 策略3: 尝试其他OA源 ({len(oa_locations)-1}个备选)...")
            #         
            #         for idx, alt_oa in enumerate(oa_locations[1:2], 1):  # 🔥 只尝试1个备选
            #             alt_pdf_url = alt_oa.get('url_for_pdf') or alt_oa.get('url')
            #             if not alt_pdf_url or alt_pdf_url == pdf_url:
            #                 continue
            #             
            #             alt_source = alt_oa.get('host_type', 'unknown')
            #             print(f"         尝试备选{idx}: {alt_source}...", flush=True)
            #             
            #             try:
            #                 alt_response = self.session.get(
            #                     alt_pdf_url,
            #                     headers=enhanced_headers,
            #                     timeout=10,  # 🔥 缩短超时: 30秒 → 10秒
            #                     allow_redirects=True
            #                 )
            #                 
            #                 if alt_response.status_code == 200 and alt_response.content.startswith(b'%PDF'):
            #                     print(f"      ✅ 策略3成功（备选{idx}）")
            #                     pdf_response = alt_response
            #                     break
            #             except Exception:
            #                 continue
            
            # 检查最终结果
            if not pdf_response or pdf_response.status_code != 200:
                if pdf_response:
                    print(f"      ⚠️  所有策略失败，最终状态码: {pdf_response.status_code}")
                else:
                    print(f"      ⚠️  所有下载策略均失败")
                return False
            
            # 验证是否为PDF
            if not pdf_response.content.startswith(b'%PDF'):
                print(f"      ⚠️  下载的文件不是有效的PDF")
                return False
            
            # 保存PDF
            with open(output_path, 'wb') as f:
                f.write(pdf_response.content)
            
            if output_path.stat().st_size > 1000:
                print(f"      ✅ Unpaywall PDF下载成功 ({output_path.stat().st_size // 1024}KB)")
                self._stat_inc('unpaywall_pdf')
                return True
            else:
                output_path.unlink()
                return False
            
        except requests.exceptions.Timeout:
            print(f"      ⚠️  Unpaywall请求超时")
        except Exception as e:
            print(f"      ⚠️  Unpaywall下载失败: {str(e)}")
        
        return False
    
    def download_from_scihub(self, doi: str, output_path: Path, year: Optional[int] = None) -> bool:
        """
        从Sci-Hub下载PDF（带年份熔断机制，优化版：缩短超时）
        
        Args:
            doi: 文章DOI
            output_path: 输出路径
            year: 文章年份（用于熔断判断）
            
        Returns:
            bool: 是否成功
        """
        if not self.enable_scihub or not doi:
            return False
        
        # 🔥 年份熔断机制：Sci-Hub在2023年后基本无新文章
        if year and self.SCIHUB_CUTOFF_YEAR is not None and year > self.SCIHUB_CUTOFF_YEAR:
            print(f"      ⏭️  文章年份{year}年 > {self.SCIHUB_CUTOFF_YEAR}年，跳过Sci-Hub（节省时间）")
            self._stat_inc('scihub_skipped')
            return False
        
        print(f"      🔗 尝试从Sci-Hub下载PDF (DOI: {doi})...", flush=True)
        
        for attempt in range(len(self.SCIHUB_MIRRORS)):
            mirror = self.SCIHUB_MIRRORS[self.current_scihub_mirror]
            
            try:
                url_formats = [
                    f"{mirror}/{doi}",
                    f"{mirror}/https://doi.org/{doi}",
                    f"{mirror}/10.{doi.split('10.')[-1]}",
                ]
                
                for format_idx, scihub_url in enumerate(url_formats):
                    try:
                        if format_idx == 0:
                            print(f"      📡 使用镜像站: {mirror}")
                        
                        response = self.session.get(
                            scihub_url, 
                            headers=self.headers, 
                            timeout=15,  # 🔥 缩短超时: 30秒 → 15秒
                            allow_redirects=True,
                            stream=False
                        )
                        
                        if response.status_code != 200:
                            continue
                        
                        # 解析HTML查找PDF链接
                        pdf_url = None
                        content = response.text
                        
                        if 'embed' in content:
                            match = re.search(r'<embed[^>]*src=["\']([^"\']+)["\']', content)
                            if match:
                                pdf_url = match.group(1)
                        
                        if not pdf_url and 'iframe' in content:
                            match = re.search(r'<iframe[^>]*src=["\']([^"\']+)["\']', content)
                            if match:
                                pdf_url = match.group(1)
                        
                        if not pdf_url:
                            match = re.search(r'((?:https?:)?//[^"\s<>]+\.pdf(?:\?[^"\s<>]*)?)', content)
                            if match:
                                pdf_url = match.group(1)
                        
                        if not pdf_url:
                            continue
                        
                        # 补全URL
                        if pdf_url.startswith('//'):
                            pdf_url = 'https:' + pdf_url
                        elif pdf_url.startswith('/'):
                            pdf_url = mirror + pdf_url
                        
                        print(f"      📄 找到PDF链接: {pdf_url[:80]}...")
                        
                        # 下载PDF（🔥 减少重试次数）
                        for retry in range(2):  # 🔥 减少重试: 3次 → 2次
                            try:
                                pdf_response = self.session.get(
                                    pdf_url, 
                                    headers=self.headers, 
                                    timeout=20,  # 🔥 缩短超时: 90秒 → 20秒
                                    stream=True
                                )
                                
                                if pdf_response.status_code == 200:
                                    break
                                elif retry < 1:  # 🔥 修改重试条件
                                    time.sleep(1)  # 🔥 缩短延迟: 2秒 → 1秒
                                    continue
                            except requests.exceptions.Timeout:
                                if retry < 1:  # 🔥 修改重试条件
                                    time.sleep(1)  # 🔥 缩短延迟: 2秒 → 1秒
                                    continue
                                else:
                                    raise
                        
                        if pdf_response.status_code == 200:
                            content_chunks = []
                            for chunk in pdf_response.iter_content(chunk_size=8192):
                                if chunk:
                                    content_chunks.append(chunk)
                            pdf_content = b''.join(content_chunks)
                            
                            if not pdf_content.startswith(b'%PDF'):
                                continue
                            
                            with open(output_path, 'wb') as f:
                                f.write(pdf_content)
                            
                            # 验证PDF完整性
                            if self.validate_pdf(output_path):
                                print(f"      ✅ Sci-Hub PDF下载成功 ({output_path.stat().st_size // 1024}KB)")
                                self._stat_inc('scihub_pdf')
                                return True
                    
                    except Exception:
                        continue
                
            except Exception:
                pass
            
            self.current_scihub_mirror = (self.current_scihub_mirror + 1) % len(self.SCIHUB_MIRRORS)
            
            if attempt < len(self.SCIHUB_MIRRORS) - 1:
                time.sleep(1)
        
        return False
    
    def download_from_publisher(self, doi: str, output_path: Path) -> bool:
        """从出版商网站下载PDF（优化版：缩短超时）"""
        if not doi:
            return False
        
        print(f"      🌐 尝试从出版商下载PDF (DOI: {doi})...", flush=True)
        
        try:
            doi_url = f"https://doi.org/{doi}"
            
            response = self.session.get(
                doi_url,
                headers=self.headers,
                timeout=10,  # 🔥 缩短超时: 30秒 → 10秒
                allow_redirects=True
            )
            
            if response.status_code != 200:
                return False
            
            final_url = response.url
            print(f"      📍 跳转到: {final_url[:60]}...")
            
            # 尝试各种PDF URL模式
            pdf_patterns = []
            
            if 'acs.org' in final_url or 'pubs.acs.org' in final_url:
                pdf_patterns.extend([
                    final_url.replace('/abs/', '/pdf/'),
                    final_url.replace('/doi/', '/doi/pdf/'),
                    f"{final_url.rstrip('/')}/pdf",
                ])
            elif 'nature.com' in final_url:
                pdf_patterns.extend([
                    final_url.replace('.html', '.pdf'),
                    f"{final_url.rstrip('/')}.pdf",
                ])
            else:
                pdf_patterns.extend([
                    f"{final_url.rstrip('/')}/pdf",
                    final_url.replace('.html', '.pdf'),
                ])
            
            for pdf_url in pdf_patterns:
                if not pdf_url:
                    continue
                
                try:
                    pdf_response = self.session.get(
                        pdf_url,
                        headers=self.headers,
                        timeout=15,  # 🔥 缩短超时: 60秒 → 15秒
                        stream=True
                    )
                    
                    if pdf_response.status_code == 200:
                        content_chunks = []
                        for chunk in pdf_response.iter_content(chunk_size=8192):
                            if chunk:
                                content_chunks.append(chunk)
                        pdf_content = b''.join(content_chunks)
                        
                        if pdf_content.startswith(b'%PDF'):
                            with open(output_path, 'wb') as f:
                                f.write(pdf_content)
                            
                            if output_path.stat().st_size > 1000:
                                print(f"      ✅ 出版商PDF下载成功 ({output_path.stat().st_size // 1024}KB)")
                                self._stat_inc('publisher_pdf')
                                return True
                            else:
                                output_path.unlink()
                        
                except Exception:
                    continue
            
            return False
            
        except Exception as e:
            print(f"      ⚠️  出版商下载失败: {str(e)[:50]}")
            return False
    

    def download_via_selenium(self, doi: str, output_path: Path) -> bool:
        """
        使用undetected-chromedriver模拟真实浏览器下载PDF（最后备选）
        
        优点：
        - ✅ 绕过Cloudflare、JS验证、机器人检测
        - ✅ 模拟真实用户行为
        - ✅ 使用undetected-chromedriver（Nature/Elsevier无法检测）
        
        缺点：
        - 速度慢（每篇需5-15秒）
        - 仍受付费墙限制
        """
        if not self.selenium_enabled:
            return False
        
        try:
            # 优先使用undetected-chromedriver
            try:
                import undetected_chromedriver as uc
                use_undetected = True
                print(f"      🌐 尝试通过undetected-chromedriver下载（绕过机器人检测）...")
            except ImportError:
                from selenium import webdriver
                from selenium.webdriver.chrome.options import Options
                from selenium.webdriver.chrome.service import Service
                from webdriver_manager.chrome import ChromeDriverManager
                use_undetected = False
                print(f"      🌐 尝试通过标准Selenium下载...")
                print(f"      💡 提示：安装undetected-chromedriver可提高成功率")
            
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            # 配置Chrome选项
            if use_undetected:
                # undetected-chromedriver配置
                options = uc.ChromeOptions()
                options.add_argument('--headless=new')  # 新版headless模式
                options.add_argument('--disable-gpu')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                
                driver = uc.Chrome(options=options, version_main=None)
            else:
                # 标准Selenium配置
                chrome_options = Options()
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
                
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
            
            driver.set_page_load_timeout(30)
            
            try:
                # 访问DOI链接
                doi_url = f"https://doi.org/{doi}"
                driver.get(doi_url)
                
                # 等待页面加载
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # 查找PDF链接
                pdf_link = None
                
                # 策略1: 查找包含"pdf"的链接
                try:
                    pdf_links = driver.find_elements(By.XPATH, 
                        "//a[contains(@href, 'pdf') or contains(text(), 'PDF') or contains(@class, 'pdf')]")
                    if pdf_links:
                        pdf_link = pdf_links[0]
                except:
                    pass
                
                # 策略2: ACS特定选择器
                if not pdf_link and 'acs.org' in driver.current_url:
                    try:
                        pdf_link = driver.find_element(By.CSS_SELECTOR, 
                            "a.article-pdfLink, a[title*='PDF']")
                    except:
                        pass
                
                if not pdf_link:
                    print(f"      ⚠️  页面中未找到PDF下载按钮")
                    driver.quit()
                    return False
                
                # 获取PDF URL
                pdf_url = pdf_link.get_attribute('href')
                print(f"      🔗 找到PDF链接: {pdf_url[:60]}...")
                
                # 获取浏览器cookies
                cookies = driver.get_cookies()
                driver.quit()
                
                # 使用带cookies的session下载
                session = requests.Session()
                for cookie in cookies:
                    session.cookies.set(cookie['name'], cookie['value'])
                
                response = session.get(pdf_url, headers=self.headers, timeout=60, stream=True)
                
                if response.status_code == 200:
                    content_chunks = []
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            content_chunks.append(chunk)
                    pdf_content = b''.join(content_chunks)
                    
                    if pdf_content.startswith(b'%PDF'):
                        with open(output_path, 'wb') as f:
                            f.write(pdf_content)
                        
                        # 验证PDF完整性
                        if self.validate_pdf(output_path):
                            print(f"      ✅ Selenium PDF下载成功 ({output_path.stat().st_size // 1024}KB)")
                            self._stat_inc('selenium_pdf')
                            return True
                
                return False
                
            finally:
                try:
                    driver.quit()
                except:
                    pass
        
        except ImportError as e:
            missing_lib = "undetected-chromedriver" if "undetected" in str(e) else "selenium"
            print(f"      ⚠️  {missing_lib}未安装，跳过（pip install {missing_lib}）")
            return False
        except Exception as e:
            print(f"      ⚠️  Selenium下载失败: {str(e)[:50]}")
            return False
    
    def convert_xml_to_markdown(self, xml_file: Path, output_file: Path) -> bool:
        """将PMC XML转换为Markdown格式"""
        try:
            print(f"      🔄 转换XML为Markdown...")
            
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            md_content = []
            
            # 提取标题
            title = root.find('.//article-title')
            if title is not None:
                md_content.append(f"# {self._get_text(title)}\n")
            
            # 提取作者
            authors = root.findall('.//contrib[@contrib-type="author"]')
            if authors:
                author_names = []
                for author in authors:
                    given = author.find('.//given-names')
                    surname = author.find('.//surname')
                    if given is not None and surname is not None:
                        author_names.append(f"{self._get_text(given)} {self._get_text(surname)}")
                
                if author_names:
                    md_content.append(f"\n**作者**: {', '.join(author_names)}\n")
            
            # 提取摘要
            abstract = root.find('.//abstract')
            if abstract is not None:
                md_content.append(f"\n## 摘要\n\n{self._get_text(abstract)}\n")
            
            # 提取正文
            body = root.find('.//body')
            if body is not None:
                md_content.append(f"\n## 正文\n")
                
                for sec in body.findall('.//sec'):
                    sec_title = sec.find('.//title')
                    if sec_title is not None:
                        md_content.append(f"\n### {self._get_text(sec_title)}\n")
                    
                    for p in sec.findall('.//p'):
                        md_content.append(f"\n{self._get_text(p)}\n")
            
            # 写入文件
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(''.join(md_content))
            
            print(f"      ✅ Markdown转换成功")
            self._stat_inc('xml_converted')
            return True
            
        except Exception as e:
            print(f"      ⚠️  XML转换失败: {e}")
            return False
    
    def _get_text(self, element) -> str:
        """递归获取元素的所有文本内容"""
        text = element.text or ''
        for child in element:
            text += self._get_text(child)
            text += child.tail or ''
        return ' '.join(text.split())
    
    def search_journal_articles(self, journal_name: str, year: int, max_results: int = 10000) -> List[str]:
        """搜索指定期刊指定年份的文章ID（带网络容错）
        
        全量模式：  搜索期刊全部文章
        免疫筛选模式：在 PubMed 服务端过滤，只返回匹配免疫关键词的文章
        """
        mode_label = "🔬 免疫筛选" if self.immune_filter_mode else "📦 全量"
        print(f"\n🔍 正在搜索期刊: {journal_name} ({year}年) [{mode_label}]")
        
        # 构建查询串
        base_query = f'"{journal_name}"[Journal] AND {year}[pdat]'
        if self.immune_filter_mode and self.immune_keywords:
            kw_query = self.build_pubmed_keyword_query()
            query = f'{base_query} AND ({kw_query})'
        else:
            query = base_query
        
        max_retries = 5  # 最多重试5次
        base_wait = 3    # 基础等待时间（秒）
        
        for attempt in range(max_retries):
            try:
                self._ncbi_acquire()
                handle = Entrez.esearch(
                    db="pubmed",
                    term=query,
                    retmax=max_results,
                    sort="pub_date",
                    retmode="xml"
                )
                
                record = Entrez.read(handle)
                handle.close()
                
                id_list = record["IdList"]
                total_count = int(record["Count"])
                
                print(f"✅ 找到 {total_count} 篇文章，准备下载 {len(id_list)} 篇")
                
                return id_list
            
            except Exception as e:
                error_msg = str(e).lower()
                
                # 判断是否为网络错误
                is_network_error = any(keyword in error_msg for keyword in [
                    'connection', 'timeout', 'network', 'unreachable', 
                    'refused', 'reset', 'broken pipe', 'timed out'
                ])
                
                if is_network_error:
                    print(f"⚠️  网络错误 (尝试 {attempt+1}/{max_retries}): {str(e)[:80]}")
                    
                    # 最后一次尝试时进入网络等待模式
                    if attempt == max_retries - 1:
                        print(f"❌ 多次重试失败，进入网络恢复等待模式...")
                        self.wait_for_network_recovery()
                        # 网络恢复后再尝试一次（额外机会）
                        try:
                            self._ncbi_acquire()
                            handle = Entrez.esearch(
                                db="pubmed",
                                term=query,
                                retmax=max_results,
                                sort="pub_date",
                                retmode="xml"
                            )
                            record = Entrez.read(handle)
                            handle.close()
                            
                            id_list = record["IdList"]
                            total_count = int(record["Count"])
                            print(f"✅ 找到 {total_count} 篇文章，准备下载 {len(id_list)} 篇")
                            return id_list
                        except Exception as final_e:
                            print(f"❌ 网络恢复后仍然失败: {str(final_e)[:100]}")
                            return []
                    
                    # 指数退避等待
                    wait_time = base_wait * (2 ** attempt)
                    print(f"   ⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    # 非网络错误（如期刊不存在），直接返回
                    print(f"❌ 搜索失败（非网络问题）: {str(e)[:100]}")
                    return []
        
        print(f"❌ 搜索最终失败: {journal_name} ({year}年)")
        return []
    
    def fetch_article_details(self, pmid_list: List[str]) -> List[Dict]:
        """批量获取文章详细信息（带网络容错）"""
        articles = []
        batch_size = 200
        
        for i in range(0, len(pmid_list), batch_size):
            batch = pmid_list[i:i+batch_size]
            print(f"📥 正在获取文章信息 {i+1}-{min(i+batch_size, len(pmid_list))}/{len(pmid_list)}")
            
            # 重试机制
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self._ncbi_acquire()
                    handle = Entrez.efetch(
                        db="pubmed",
                        id=batch,
                        rettype="xml",
                        retmode="xml"
                    )
                    
                    records = Entrez.read(handle)
                    handle.close()
                    
                    for article in records['PubmedArticle']:
                        try:
                            medline = article['MedlineCitation']
                            article_data = medline['Article']
                            
                            pmid = str(medline['PMID'])
                            title = article_data.get('ArticleTitle', 'No Title')
                            doi = self.get_doi_from_article(article_data)
                            
                            journal = article_data['Journal']
                            journal_title = journal.get('Title', 'Unknown Journal')
                            
                            issue_info = journal.get('JournalIssue', {})
                            volume = issue_info.get('Volume', '')
                            issue = issue_info.get('Issue', '')
                            
                            pub_date = issue_info.get('PubDate', {})
                            year = pub_date.get('Year', '')
                            month = pub_date.get('Month', '')
                            
                            abstract_list = article_data.get('Abstract', {}).get('AbstractText', [])
                            abstract = ' '.join([str(text) for text in abstract_list])
                            
                            author_list = article_data.get('AuthorList', [])
                            authors = []
                            for author in author_list:
                                if 'LastName' in author and 'ForeName' in author:
                                    authors.append(f"{author['ForeName']} {author['LastName']}")
                            
                            if issue:
                                issue_label = f"Issue{issue}"
                            elif volume:
                                issue_label = f"Vol{volume}"
                            else:
                                issue_label = "Unknown_Issue"
                            
                            articles.append({
                                'pmid': pmid,
                                'title': title,
                                'doi': doi,
                                'journal': journal_title,
                                'volume': volume,
                                'issue': issue,
                                'year': year,
                                'month': month,
                                'issue_label': issue_label,
                                'abstract': abstract,
                                'authors': authors
                            })
                            
                        except Exception as e:
                            continue
                    
                    break  # 成功则跳出重试循环
                    
                except Exception as e:
                    error_msg = str(e).lower()
                    is_network_error = any(keyword in error_msg for keyword in [
                        'connection', 'timeout', 'network', 'unreachable', 
                        'refused', 'reset', 'broken pipe', 'timed out'
                    ])
                    
                    if is_network_error and attempt < max_retries - 1:
                        wait_time = 2 * (attempt + 1)
                        print(f"   ⚠️  网络错误，{wait_time}秒后重试...")
                        time.sleep(wait_time)
                    elif attempt == max_retries - 1:
                        print(f"   ❌ 批量获取失败: {e}")
                        # 检查网络
                        if not self.check_network_health():
                            self.wait_for_network_recovery()
                    else:
                        print(f"   ❌ 获取失败: {e}")
                        break
        
        return articles
    
    def batch_query_pmc_ids(self, pmid_list: list) -> dict:
        """
        批量查询 PMC ID（替代逐篇 elink，大幅提速）
        每次 API 调用处理最多 200 个 PMID，返回 {pmid: pmc_id} 字典。
        """
        pmc_map = {}
        batch_size = 200
        for i in range(0, len(pmid_list), batch_size):
            batch = pmid_list[i:i + batch_size]
            for attempt in range(3):
                try:
                    self._ncbi_acquire()
                    handle = Entrez.elink(
                        dbfrom="pubmed",
                        db="pmc",
                        id=",".join(str(p) for p in batch),
                        retmode="xml"
                    )
                    records = Entrez.read(handle)
                    handle.close()
                    for linkset in records:
                        id_list = linkset.get('IdList', [])
                        src_id = str(id_list[0]) if id_list else None
                        if src_id and 'LinkSetDb' in linkset:
                            for link_db in linkset['LinkSetDb']:
                                if link_db.get('DbTo') == 'pmc' and link_db.get('Link'):
                                    pmc_map[src_id] = str(link_db['Link'][0]['Id'])
                                    break
                    break  # 成功则退出重试
                except Exception as e:
                    if attempt < 2:
                        print(f"   ⚠️  批量查PMC ID重试 {attempt+1}/3: {e}")
                        time.sleep(2 ** attempt)
                    else:
                        print(f"   ⚠️  批量查PMC ID失败（这批{len(batch)}篇将逐篇回退查询）: {e}")
        return pmc_map

    def batch_check_unpaywall_oa(self, doi_list: list, max_workers: int = 3) -> dict:
        """
        并发批量检查 Unpaywall OA 状态，返回 {doi: True/False}。
        True=有OA全文可下载，False=非OA直接跳过。
        用 ThreadPoolExecutor 并发查询，8线程时速度约为串行的8倍。
        """
        oa_map = {}
        if not doi_list or not self.enable_unpaywall:
            return oa_map

        def _check_one(doi):
            try:
                url = f"https://api.unpaywall.org/v2/{doi}"
                print(f"      🔍 Unpaywall查询: {doi[:50]}...", flush=True)
                resp = self.session.get(url, params={'email': self.email},
                                        headers=self.headers, timeout=8)
                if resp.status_code == 200:
                    data = resp.json()
                    is_oa = bool(data.get('is_oa', False))
                    # 进一步确认：is_oa=True 时还需要有可用的 PDF URL
                    if is_oa:
                        best = data.get('best_oa_location') or {}
                        has_pdf = bool(best.get('url_for_pdf') or best.get('url'))
                        print(f"         {'✅ OA可下载' if has_pdf else '⚠️ OA但无PDF'}: {doi[:40]}", flush=True)
                        return doi, has_pdf
                    print(f"         ❌ 非OA: {doi[:40]}", flush=True)
                    return doi, False
                print(f"         ⚠️ HTTP {resp.status_code}: {doi[:40]}", flush=True)
                return doi, False
            except Exception as e:
                print(f"         ⚠️ 查询失败({type(e).__name__}): {doi[:40]}", flush=True)
                return doi, None  # None = 查询失败，保留正常下载流程

        # as_completed 设置总超时（单个 doi timeout=8s，最多等 max_workers 轮 + 缓冲）
        total_timeout = max(60, len(doi_list) * 10 / max(max_workers, 1))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_check_one, doi): doi for doi in doi_list}
            try:
                for future in as_completed(futures, timeout=total_timeout):
                    try:
                        doi_result, status = future.result()
                        if status is not None:  # None表示网络失败，不写入map（保留正常流程）
                            oa_map[doi_result] = status
                    except Exception:
                        pass
            except TimeoutError:
                print(f"  ⚠️  Unpaywall 批量预查超时（{total_timeout:.0f}s），跳过未完成的查询，继续正常下载流程")
                for future in futures:
                    if not future.done():
                        future.cancel()

        return oa_map

    def download_full_text(self, pmid: str, doi: Optional[str], output_dir: Path, base_filename: str, year: Optional[int] = None, pmc_id: Optional[str] = None, pre_oa: Optional[bool] = None) -> Tuple[bool, str]:
        """
        智能5级下载策略（XML优先，AI训练最佳）：
        PMC XML → PMC PDF → Unpaywall → Sci-Hub(年份熔断) → Publisher → Selenium
        
        Args:
            pmc_id: 预查好的PMC ID（来自 batch_query_pmc_ids），为None时回退逐篇查询
            pre_oa: 预查好的Unpaywall OA状态（True=OA可下载，False=非OA直接跳过，None=未预查）
        """
        pdf_file = output_dir / f"{base_filename}.pdf"
        xml_file = output_dir / f"{base_filename}.xml"
        md_file = output_dir / f"{base_filename}.md"
        
        # 查找PMC ID：优先使用预查结果，否则回退到逐篇 elink
        if pmc_id is None:
            try:
                self._ncbi_acquire()
                handle = Entrez.elink(
                    dbfrom="pubmed",
                    db="pmc",
                    id=pmid,
                    retmode="xml"
                )
                record = Entrez.read(handle)
                handle.close()
                for linkset in record:
                    if 'LinkSetDb' in linkset:
                        for link_db in linkset['LinkSetDb']:
                            if link_db['DbTo'] == 'pmc':
                                pmc_id = link_db['Link'][0]['Id']
                                break
            except:
                pass
        
        # 🔥 Level 1: PMC XML（AI训练最佳，优先级最高）
        if pmc_id and self.prefer_xml:
            if self.download_pmc_xml(pmc_id, xml_file, md_file):
                return (True, "PMC_XML")
        
        # Level 2: PMC PDF
        if pmc_id:
            if self.download_pmc_pdf(pmc_id, pdf_file):
                return (True, "PMC_PDF")
        
        # Level 3: Unpaywall（合法OA，推荐）
        # pre_oa=False 表示预查已确认非OA，直接跳过，节省10秒
        if doi and self.enable_unpaywall and pre_oa is not False:
            if self.download_from_unpaywall(doi, pdf_file):
                return (True, "UNPAYWALL_PDF")
        elif pre_oa is False:
            print(f"      ⏭️  预查确认非OA，跳过Unpaywall+Publisher")
        
        # Level 4: Sci-Hub（带年份熔断）
        if doi and self.enable_scihub:
            if self.download_from_scihub(doi, pdf_file, year):
                return (True, "SCIHUB_PDF")
        
        # Level 5: Publisher（pre_oa=False 时也跳过，出版商网站同样不会给全文）
        if doi and pre_oa is not False:
            if self.download_from_publisher(doi, pdf_file):
                return (True, "PUBLISHER_PDF")
        
        # Level 6: Selenium（最慢，最后备选）
        if doi and self.selenium_enabled:
            print(f"      🚨 前5种方法均失败，启用浏览器自动化（较慢）...")
            if self.download_via_selenium(doi, pdf_file):
                return (True, "SELENIUM_PDF")
        
        # 所有方法都失败
        return (False, "NONE")
    
    def save_article_text(self, article: Dict, output_dir: Path, pmc_id_map: dict = None, oa_status_map: dict = None):
        """保存文章信息并智能下载全文（线程安全）"""
        pmid = article['pmid']
        doi = article.get('doi')
        title = self.sanitize_filename(article['title'])
        year = int(article.get('year', 0)) if article.get('year') else None
        
        # ── 跨期刊名 PMID 去重检查（线程安全）──
        pmid_str = str(pmid)
        with self._pmids_lock:
            if pmid_str in self.downloaded_pmids:
                print(f"      ⏭️  PMID {pmid} 已下载（另一期刊名已处理），跳过重复")
                # _stat_inc 内部已加锁；这里避免重复加锁导致死锁
                self._stat_inc('dedup_skipped')
                return
            # 预先占位，防止并发中同一 PMID 被两个线程同时处理
            self.downloaded_pmids.add(pmid_str)
        
        base_filename = f"{pmid}_{title[:100]}"
        
        # 创建摘要文本文件
        txt_file = output_dir / f"{base_filename}.txt"
        
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write(f"PMID: {pmid}\n")
            f.write(f"标题: {article['title']}\n")
            f.write(f"DOI: {doi or 'N/A'}\n")
            f.write(f"期刊: {article['journal']}\n")
            f.write(f"卷号: {article['volume']}\n")
            f.write(f"期号: {article['issue']}\n")
            f.write(f"年份: {article['year']}\n")
            f.write(f"作者: {', '.join(article['authors'])}\n")
            f.write(f"\n{'='*80}\n")
            f.write(f"摘要:\n{article['abstract']}\n")
            f.write(f"\n{'='*80}\n")
            f.write(f"PubMed链接: https://pubmed.ncbi.nlm.nih.gov/{pmid}/\n")
            if doi:
                f.write(f"DOI链接: https://doi.org/{doi}\n")
        
        # 从预查字典获取 PMC ID（没有时传 None，内部回退逐篇查询）
        pre_pmc_id = (pmc_id_map or {}).get(str(pmid))
        # 从预查字典获取 OA 状态（None 表示未预查，需正常尝试；False 表示已确认非OA，跳过）
        pre_oa = (oa_status_map or {}).get(doi) if doi else None

        # 打印开始下载（让日志实时可见，不再静默等待）
        pmc_tag  = f"PMC:{pre_pmc_id}" if pre_pmc_id else "无PMC"
        oa_tag   = ("OA✓" if pre_oa else ("非OA" if pre_oa is False else "OA?"))
        print(f"      ⬇️  PMID {pmid} [{pmc_tag}|{oa_tag}] {article['title'][:45]}...", flush=True)

        # 尝试下载全文
        self._stat_inc('total')
        success, source = self.download_full_text(pmid, doi, output_dir, base_filename, year, pmc_id=pre_pmc_id, pre_oa=pre_oa)
        
        # 持久化 PMID 记录（避免同一文章被另一期刊名再次处理）
        self._append_downloaded_pmid(pmid_str)
        
        if success:
            print(f"      ✅ 全文已下载 [{source}]", flush=True)
            # 保存元数据（用于RAG检索）
            self.save_metadata(article, output_dir, base_filename, source)
        else:
            print(f"      ℹ️  仅摘要（全文不可用）", flush=True)
            self._stat_inc('abstract_only')
            with self._failed_lock:
                self.failed_articles.append({
                    'pmid': pmid,
                    'title': article['title'],
                    'doi': doi,
                    'journal': article['journal'],
                    'year': article['year'],
                    'pubmed_url': f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                    'doi_url': f"https://doi.org/{doi}" if doi else 'N/A'
                })
    
    def _load_downloaded_pmids(self) -> set:
        """从持久化文件加载已下载 PMID 集合（跨运行去重）"""
        if self.downloaded_pmids_file.exists():
            try:
                with open(self.downloaded_pmids_file, 'r', encoding='utf-8') as f:
                    return set(line.strip() for line in f if line.strip())
            except Exception as e:
                print(f"⚠️  加载 PMID 去重记录失败: {e}，将重新初始化")
        return set()
    
    def _append_downloaded_pmid(self, pmid: str):
        """将一个 PMID 追加到持久化去重文件"""
        with open(self.downloaded_pmids_file, 'a', encoding='utf-8') as f:
            f.write(pmid + '\n')
    
    def _load_global_stats(self) -> Dict:
        """加载全局统计（从JSON文件）"""
        if self.global_stats_file.exists():
            try:
                with open(self.global_stats_file, 'r', encoding='utf-8') as f:
                    stats = json.load(f)
                    # 转换时间字符串为datetime对象
                    if stats.get('first_run_time'):
                        stats['first_run_time'] = datetime.fromisoformat(stats['first_run_time'])
                    if stats.get('last_update'):
                        stats['last_update'] = datetime.fromisoformat(stats['last_update'])
                    if stats.get('current_session_start'):
                        stats['current_session_start'] = datetime.fromisoformat(stats['current_session_start'])
                    return stats
            except Exception as e:
                print(f"⚠️  加载全局统计失败: {e}，将重新初始化")
        
        # 返回默认初始值
        return {
            'first_run_time': datetime.now(),
            'last_update': None,
            'current_session_start': datetime.now(),
            'total_sessions': 0,
            'completed_journal_years': [],  # 已完成的 "期刊名_年份" 列表
            'total_articles': 0,
            'pmc_xml': 0,
            'pmc_pdf': 0,
            'unpaywall_pdf': 0,
            'scihub_pdf': 0,
            'scihub_skipped': 0,
            'publisher_pdf': 0,
            'selenium_pdf': 0,
            'abstract_only': 0,
            'corrupted_removed': 0,
            'dedup_skipped': 0
        }
    
    def _save_global_stats(self):
        """
        保存全局统计到JSON文件（内部方法，调用者需持有锁）
        🔥 注意：此方法不应该独立使用锁，因为调用者已经持有锁
        """
        # 🔥 移除嵌套锁，避免死锁！
        # 转换datetime对象为字符串
        stats_to_save = self.global_stats.copy()
        if stats_to_save.get('first_run_time'):
            stats_to_save['first_run_time'] = stats_to_save['first_run_time'].isoformat()
        if stats_to_save.get('last_update'):
            stats_to_save['last_update'] = stats_to_save['last_update'].isoformat()
        if stats_to_save.get('current_session_start'):
            stats_to_save['current_session_start'] = stats_to_save['current_session_start'].isoformat()
        
        with open(self.global_stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats_to_save, f, ensure_ascii=False, indent=2)
    
    def update_global_stats(self, journal_name: str, year: int):
        """更新全局统计（线程安全）"""
        with self.global_stats_lock:
            # 标记为已完成
            journal_year_key = f"{journal_name}_{year}"
            if journal_year_key not in self.global_stats['completed_journal_years']:
                self.global_stats['completed_journal_years'].append(journal_year_key)
            
            # 累加统计
            self.global_stats['total_articles'] += self.stats['total']
            self.global_stats['pmc_xml'] += self.stats['pmc_xml']
            self.global_stats['pmc_pdf'] += self.stats['pmc_pdf']
            self.global_stats['unpaywall_pdf'] += self.stats['unpaywall_pdf']
            self.global_stats['scihub_pdf'] += self.stats['scihub_pdf']
            self.global_stats['scihub_skipped'] += self.stats['scihub_skipped']
            self.global_stats['publisher_pdf'] += self.stats['publisher_pdf']
            self.global_stats['selenium_pdf'] += self.stats['selenium_pdf']
            self.global_stats['abstract_only'] += self.stats['abstract_only']
            self.global_stats['corrupted_removed'] += self.stats['corrupted_removed']
            self.global_stats['dedup_skipped'] = self.global_stats.get('dedup_skipped', 0) + self.stats['dedup_skipped']
            self.global_stats['last_update'] = datetime.now()
            
            # 保存到文件
            self._save_global_stats()
    
    def save_global_report(self, total_journal_years: int = None):
        """
        保存全局统计报告（持续更新）
        
        Args:
            total_journal_years: 总任务数（期刊数×年份数）
        """
        with self.global_stats_lock:
            stats = self.global_stats.copy()
        
        completed = len(stats['completed_journal_years'])
        
        with open(self.global_report_file, 'w', encoding='utf-8') as f:
            f.write(f"PubMed文献下载全局统计报告（跨运行持久化）\n")
            f.write(f"{'='*80}\n\n")
            
            # 时间信息
            if stats.get('first_run_time'):
                f.write(f"首次运行: {stats['first_run_time'].strftime('%Y-%m-%d %H:%M:%S')}\n")
            if stats.get('last_update'):
                f.write(f"最后更新: {stats['last_update'].strftime('%Y-%m-%d %H:%M:%S')}\n")
            if stats.get('current_session_start'):
                session_elapsed = (datetime.now() - stats['current_session_start']).total_seconds()
                f.write(f"本次会话: {session_elapsed/3600:.1f} 小时\n")
            if stats.get('first_run_time') and stats.get('last_update'):
                total_elapsed = (stats['last_update'] - stats['first_run_time']).total_seconds()
                f.write(f"累计运行: {total_elapsed/3600:.1f} 小时 (跨 {stats.get('total_sessions', 0)} 次会话)\n")
            
            f.write(f"\n{'='*80}\n")
            f.write(f"任务进度:\n\n")
            
            # 总体进度
            if total_journal_years:
                progress_pct = (completed / total_journal_years) * 100
                remaining = total_journal_years - completed
                f.write(f"  📊 总任务进度: {completed}/{total_journal_years} ({progress_pct:.1f}%)\n")
                f.write(f"  ✅ 已完成: {completed} 个期刊年份\n")
                f.write(f"  ⏳ 剩余: {remaining} 个期刊年份\n")
                
                # 预估剩余时间
                if stats.get('first_run_time') and stats.get('last_update') and completed > 0:
                    total_elapsed = (stats['last_update'] - stats['first_run_time']).total_seconds()
                    avg_time_per_journal_year = total_elapsed / completed
                    estimated_remaining_hours = (remaining * avg_time_per_journal_year) / 3600
                    f.write(f"  ⏱️  预计剩余: {estimated_remaining_hours:.1f} 小时\n")
            else:
                f.write(f"  ✅ 已完成: {completed} 个期刊年份\n")
            
            f.write(f"  📄 累计处理文章: {stats['total_articles']:,} 篇\n\n")
            
            f.write(f"{'='*80}\n")
            f.write(f"全文获取统计:\n\n")
            f.write(f"  📄 PMC XML: {stats['pmc_xml']} 篇 ⭐ (AI训练最佳)\n")
            f.write(f"  📕 PMC PDF: {stats['pmc_pdf']} 篇\n")
            f.write(f"  🔓 Unpaywall OA: {stats['unpaywall_pdf']} 篇\n")
            f.write(f"  📕 Sci-Hub PDF: {stats['scihub_pdf']} 篇\n")
            if stats['scihub_skipped'] > 0:
                f.write(f"      ⏭️  Sci-Hub跳过: {stats['scihub_skipped']} 篇 (年份熔断)\n")
            f.write(f"  📕 出版商PDF: {stats['publisher_pdf']} 篇\n")
            f.write(f"  🌐 Selenium PDF: {stats['selenium_pdf']} 篇\n")
            f.write(f"  📋 仅摘要: {stats['abstract_only']} 篇\n")
            if stats['corrupted_removed'] > 0:
                f.write(f"  🗑️  移除坏文件: {stats['corrupted_removed']} 个\n")
            if stats.get('dedup_skipped', 0) > 0:
                f.write(f"  ⏭️  跨期刊名去重跳过: {stats['dedup_skipped']} 篇\n")
            f.write(f"  ━━━━━━━━━━━━━━━━━\n")
            f.write(f"  📊 总计: {stats['total_articles']} 篇\n\n")
            
            if stats['total_articles'] > 0:
                fulltext_count = (stats['pmc_xml'] + stats['pmc_pdf'] + 
                                stats['unpaywall_pdf'] + stats['scihub_pdf'] + 
                                stats['publisher_pdf'] + stats['selenium_pdf'])
                fulltext_rate = fulltext_count / stats['total_articles'] * 100
                f.write(f"  ✅ 全文获取率: {fulltext_rate:.1f}% ({fulltext_count}/{stats['total_articles']})\n")
                
                if stats['scihub_skipped'] > 0:
                    time_saved_hours = (stats['scihub_skipped'] * 15) / 3600
                    f.write(f"  ⏱️  熔断节省时间: 约 {time_saved_hours:.1f} 小时\n")
                
                # 平均速度
                if stats['first_run_time'] and stats['last_update']:
                    elapsed = (stats['last_update'] - stats['first_run_time']).total_seconds()
                    if elapsed > 0:
                        speed = stats['total_articles'] / (elapsed / 3600)
                        f.write(f"  ⚡ 平均速度: {speed:.1f} 篇/小时\n")
                
                f.write("\n")
            
            # 最近完成的10个任务
            f.write(f"{'='*80}\n")
            f.write(f"最近完成（最后10个）:\n\n")
            recent_completed = stats['completed_journal_years'][-10:]
            for idx, jy in enumerate(reversed(recent_completed), 1):
                parts = jy.rsplit('_', 1)
                if len(parts) == 2:
                    f.write(f"  {idx:2d}. {parts[0]} ({parts[1]}年)\n")
            
            if not stats['completed_journal_years']:
                f.write(f"  (暂无)\n")
            
            f.write("\n")
        
        # 方法结束，不输出（避免阻塞）
    
    def process_journal(self, journal_name: str, year: int) -> bool:
        """
        处理单个期刊（返回是否成功）
        
        Returns:
            bool: 是否成功处理
        """
        print(f"\n{'='*80}")
        print(f"📚 开始处理期刊: {journal_name} ({year}年)")
        print(f"{'='*80}")
        
        # ===== 🔄 智能跳过：检查是否已下载完成 =====
        safe_journal_name = self.sanitize_filename(journal_name)
        pattern = f"{safe_journal_name}--{year}--*"
        existing_folders = list(self.download_dir.glob(pattern))
        
        if existing_folders:
            # 检查是否有实际内容（至少有1个.txt文件）
            has_content = False
            total_files = 0
            for folder in existing_folders:
                txt_files = list(folder.glob("*.txt"))
                total_files += len(txt_files)
                if len(txt_files) > 0:
                    has_content = True
            
            if has_content:
                print(f"✅ 检测到已下载内容:")
                print(f"   📁 文件夹数: {len(existing_folders)}")
                print(f"   📄 文章数: {total_files}")
                print(f"   ⏭️  跳过此期刊（如需重新下载，请手动删除文件夹）\n")
                return True  # 已完成视为成功
            else:
                print(f"⚠️  检测到空文件夹，将重新下载\n")
        
        # 重置统计
        self.stats = {
            'total': 0,
            'pmc_xml': 0,
            'pmc_pdf': 0,
            'unpaywall_pdf': 0,
            'scihub_pdf': 0,
            'scihub_skipped': 0,
            'publisher_pdf': 0,
            'selenium_pdf': 0,
            'xml_converted': 0,
            'abstract_only': 0,
            'failed': 0,
            'corrupted_removed': 0,
            'dedup_skipped': 0
        }
        self.failed_articles = []
        self.current_year = year  # 设置当前年份用于熔断
        
        pmid_list = self.search_journal_articles(journal_name, year)
        
        # 🔥 关键：如果搜索失败（返回空列表），不标记为完成
        if not pmid_list:
            print(f"⚠️  未找到文章，可能是网络问题或期刊无文章")
            return False  # 返回失败，下次重试
        
        articles = self.fetch_article_details(pmid_list)
        
        if not articles:
            print(f"⚠️  未能获取文章详情，可能是网络问题")
            return False  # 返回失败，下次重试
        
        # ===== 🚀 批量预查所有文章的 PMC ID（核心提速优化）=====
        all_pmids = [str(a['pmid']) for a in articles]
        print(f"⚡ 批量预查 {len(all_pmids)} 篇文章的PMC ID（单次API代替{len(all_pmids)}次）...", flush=True)
        pmc_id_map = self.batch_query_pmc_ids(all_pmids)
        pmc_hit = sum(1 for p in all_pmids if p in pmc_id_map)
        print(f"   ✅ PMC命中: {pmc_hit}/{len(all_pmids)} 篇 ({pmc_hit*100//max(len(all_pmids),1)}%)", flush=True)

        # ===== 🚀 对无PMC的文章，并发预查 Unpaywall OA 状态（跳过非OA文章的无效等待）=====
        no_pmc_articles = [a for a in articles if str(a['pmid']) not in pmc_id_map and a.get('doi')]
        oa_status_map = {}  # {doi: True/False}
        if no_pmc_articles and self.enable_unpaywall:
            print(f"⚡ 并发预查 {len(no_pmc_articles)} 篇无PMC文章的Unpaywall OA状态（3线程）...", flush=True)
            oa_status_map = self.batch_check_unpaywall_oa([a['doi'] for a in no_pmc_articles])
            oa_hit = sum(1 for v in oa_status_map.values() if v)
            print(f"   ✅ OA命中: {oa_hit}/{len(no_pmc_articles)} 篇，"
                  f"跳过非OA: {len(no_pmc_articles)-oa_hit} 篇（节省约"
                  f" {(len(no_pmc_articles)-oa_hit)*25//60} 分钟）", flush=True)

        # 按期号分组
        issue_groups = {}
        for article in articles:
            issue_label = article['issue_label']
            if issue_label not in issue_groups:
                issue_groups[issue_label] = []
            issue_groups[issue_label].append(article)
        
        # 并发线程数：固定 2 线程，避免多线程同时挂住导致整批卡死
        CONCURRENT_WORKERS = 2

        def _save_one(article, issue_dir):
            """单篇文章下载任务（线程安全：_ncbi_acquire 内有锁）"""
            self.save_article_text(article, issue_dir, pmc_id_map=pmc_id_map, oa_status_map=oa_status_map)
            return article

        def _run_concurrent(articles_list, issue_dir, label):
            """并发执行文章下载，返回完成数"""
            total = len(articles_list)
            done_count = [0]
            lock = threading.Lock()
            # 单篇最长等待：120秒（含所有下载策略的超时之和）
            PER_ARTICLE_TIMEOUT = 120

            with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
                futures = {executor.submit(_save_one, a, issue_dir): a for a in articles_list}
                try:
                    for future in as_completed(futures, timeout=PER_ARTICLE_TIMEOUT * total):
                        article = futures[future]
                        with lock:
                            done_count[0] += 1
                            idx = done_count[0]
                        try:
                            future.result(timeout=PER_ARTICLE_TIMEOUT)
                            print(f"  [{idx}/{total}] ({idx*100//total}%) ✅ {article['title'][:50]}...", flush=True)
                        except TimeoutError:
                            print(f"  [{idx}/{total}] ⚠️  下载超时跳过: {article['title'][:50]}...", flush=True)
                        except Exception as e:
                            print(f"  [{idx}/{total}] ⚠️  {article['title'][:50]}... 失败: {e}", flush=True)
                        if idx % 10 == 0 or idx == total:
                            self._print_progress_stats()
                except TimeoutError:
                    print(f"  ⚠️  批次整体超时（{PER_ARTICLE_TIMEOUT * total}s），取消剩余任务继续下一期刊", flush=True)
                    for f in futures:
                        f.cancel()
            return done_count[0]

        # 保存文章（并发处理）
        total_saved = 0
        for issue_label, issue_articles in issue_groups.items():
            safe_journal_folder = self.sanitize_filename(f"{journal_name}--{year}--{issue_label}")
            
            if self.immune_filter_mode:
                # ---- 免疫筛选模式 ----
                print(f"\n🔎 关键词归类中: {journal_name} ({year}年) / 期号 {issue_label} / {len(issue_articles)} 篇", flush=True)
                kw_article_groups = {}
                for article in issue_articles:
                    best_kw = self.find_best_keyword(article)
                    article['_matched_keyword'] = best_kw
                    if best_kw not in kw_article_groups:
                        kw_article_groups[best_kw] = []
                    kw_article_groups[best_kw].append(article)
                
                for kw_dir_name, kw_articles in kw_article_groups.items():
                    issue_dir = self.download_dir / kw_dir_name / safe_journal_folder
                    issue_dir.mkdir(parents=True, exist_ok=True)
                    print(f"\n📁 保存到: {kw_dir_name}/{safe_journal_folder} ({len(kw_articles)}篇) [⚡{CONCURRENT_WORKERS}线程并发]", flush=True)
                    saved = _run_concurrent(kw_articles, issue_dir, kw_dir_name)
                    total_saved += saved
            else:
                # ---- 全量模式 ----
                print(f"\n📁 保存到文件夹: {safe_journal_folder} ({len(issue_articles)}篇) [⚡{CONCURRENT_WORKERS}线程并发]", flush=True)
                issue_dir = self.download_dir / safe_journal_folder
                issue_dir.mkdir(exist_ok=True)
                saved = _run_concurrent(issue_articles, issue_dir, safe_journal_folder)
                total_saved += saved
        
        print(f"\n✅ 期刊 {journal_name} ({year}年) 处理完成！共保存 {total_saved} 篇文章")
        self._print_final_stats()
        
        print(f"\n🔄 生成期刊报告...", flush=True)
        self.generate_journal_report(journal_name, year, issue_groups)
        
        print(f"🔄 更新全局统计...", flush=True)
        # 更新全局统计（传入期刊名和年份）
        try:
            self.update_global_stats(journal_name, year)
            print(f"✅ 全局统计更新成功", flush=True)
        except Exception as e:
            print(f"⚠️  全局统计更新失败: {str(e)[:100]}", flush=True)
        
        print(f"🔄 保存全局报告...", flush=True)
        # 保存全局报告（需要在process_journal_list中设置总任务数）
        try:
            if hasattr(self, 'total_journal_years'):
                self.save_global_report(self.total_journal_years)
            else:
                self.save_global_report()
            print(f"✅ 全局报告保存成功", flush=True)
        except Exception as e:
            print(f"⚠️  全局报告保存失败: {str(e)[:100]}", flush=True)
        
        print(f"✅ process_journal 即将返回 True\n", flush=True)
        return True  # 成功处理
    
    def _print_progress_stats(self):
        """打印实时进度统计"""
        xml_total = self.stats['pmc_xml']
        pdf_total = self.stats['pmc_pdf'] + self.stats['unpaywall_pdf'] + self.stats['scihub_pdf'] + self.stats['publisher_pdf'] + self.stats['selenium_pdf']
        print(f"      📊 进度统计: XML={xml_total}, PDF={pdf_total}, "
              f"摘要={self.stats['abstract_only']}, "
              f"已清理坏文件={self.stats['corrupted_removed']}")
    
    def _print_final_stats(self):
        """打印最终统计"""
        print(f"\n📊 下载统计:")
        print(f"   📄 PMC XML: {self.stats['pmc_xml']} 篇 ⭐ (AI训练最佳)")
        print(f"   📕 PMC PDF: {self.stats['pmc_pdf']} 篇")
        print(f"   🔓 Unpaywall OA: {self.stats['unpaywall_pdf']} 篇")
        print(f"   📕 Sci-Hub PDF: {self.stats['scihub_pdf']} 篇")
        if self.stats['scihub_skipped'] > 0:
            print(f"      ⏭️  Sci-Hub跳过: {self.stats['scihub_skipped']} 篇 (年份熔断)")
        print(f"   📕 出版商PDF: {self.stats['publisher_pdf']} 篇")
        print(f"   🌐 Selenium PDF: {self.stats['selenium_pdf']} 篇")
        print(f"   📋 仅摘要: {self.stats['abstract_only']} 篇")
        if self.stats['corrupted_removed'] > 0:
            print(f"   🗑️  移除坏文件: {self.stats['corrupted_removed']} 个")
        if self.stats['dedup_skipped'] > 0:
            print(f"   ⏭️  跨期刊名去重跳过: {self.stats['dedup_skipped']} 篇")
        print(f"   ━━━━━━━━━━━━━━━━━")
        print(f"   📊 总计: {self.stats['total']} 篇")
        
        if self.stats['total'] > 0:
            fulltext_count = self.stats['pmc_xml'] + self.stats['pmc_pdf'] + self.stats['unpaywall_pdf'] + self.stats['scihub_pdf'] + self.stats['publisher_pdf'] + self.stats['selenium_pdf']
            fulltext_rate = fulltext_count / self.stats['total'] * 100
            print(f"   ✅ 全文获取率: {fulltext_rate:.1f}% ({fulltext_count}/{self.stats['total']})")
            
            if self.stats['scihub_skipped'] > 0:
                time_saved_hours = (self.stats['scihub_skipped'] * 15) / 3600  # 假设每篇节省15秒
                print(f"   ⏱️  熔断节省时间: 约 {time_saved_hours:.1f} 小时")
    
    def generate_journal_report(self, journal_name: str, year: int, issue_groups: Dict):
        """生成期刊详细报告"""
        report_file = self.download_dir / f"_报告_{self.sanitize_filename(journal_name)}_{year}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"期刊下载详细报告 v4.0\n")
            f.write(f"{'='*80}\n\n")
            f.write(f"期刊名称: {journal_name}\n")
            f.write(f"年份: {year}\n")
            f.write(f"下载时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write(f"{'='*80}\n")
            f.write(f"全文获取统计:\n\n")
            f.write(f"  📄 PMC XML: {self.stats['pmc_xml']} 篇 ⭐ (AI训练最佳)\n")
            f.write(f"  📕 PMC PDF: {self.stats['pmc_pdf']} 篇\n")
            f.write(f"  🔓 Unpaywall OA: {self.stats['unpaywall_pdf']} 篇\n")
            f.write(f"  📕 Sci-Hub PDF: {self.stats['scihub_pdf']} 篇\n")
            if self.stats['scihub_skipped'] > 0:
                f.write(f"      ⏭️  Sci-Hub跳过: {self.stats['scihub_skipped']} 篇 (年份熔断)\n")
            f.write(f"  📕 出版商PDF: {self.stats['publisher_pdf']} 篇\n")
            f.write(f"  🌐 Selenium PDF: {self.stats['selenium_pdf']} 篇\n")
            f.write(f"  📋 仅摘要: {self.stats['abstract_only']} 篇\n")
            if self.stats['corrupted_removed'] > 0:
                f.write(f"  🗑️  移除坏文件: {self.stats['corrupted_removed']} 个\n")
            if self.stats['dedup_skipped'] > 0:
                f.write(f"  ⏭️  跨期刊名去重跳过: {self.stats['dedup_skipped']} 篇\n")
            f.write(f"  ━━━━━━━━━━━━━━━━━\n")
            f.write(f"  📊 总计: {self.stats['total']} 篇\n\n")
            
            if self.stats['total'] > 0:
                fulltext_count = self.stats['pmc_xml'] + self.stats['pmc_pdf'] + self.stats['unpaywall_pdf'] + self.stats['scihub_pdf'] + self.stats['publisher_pdf'] + self.stats['selenium_pdf']
                fulltext_rate = fulltext_count / self.stats['total'] * 100
                f.write(f"  ✅ 全文获取率: {fulltext_rate:.1f}%\n")
                
                if self.stats['scihub_skipped'] > 0:
                    time_saved_hours = (self.stats['scihub_skipped'] * 15) / 3600
                    f.write(f"  ⏱️  熔断节省时间: 约 {time_saved_hours:.1f} 小时\n")
                f.write("\n")
        
        # 导出失败清单
        if self.failed_articles:
            csv_file = self.download_dir / f"_失败清单_{self.sanitize_filename(journal_name)}_{year}.csv"
            with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['pmid', 'title', 'doi', 'journal', 'year', 'pubmed_url', 'doi_url'])
                writer.writeheader()
                writer.writerows(self.failed_articles)
            
            print(f"\n📋 失败清单已导出: {csv_file.name}", flush=True)
    
    def download_article_with_retry(self, article: Dict, output_dir: Path) -> Dict:
        """下载单篇文章（带重试，用于并发）"""
        try:
            self.save_article_text(article, output_dir)
            return {'success': True, 'pmid': article['pmid']}
        except Exception as e:
            return {'success': False, 'pmid': article['pmid'], 'error': str(e)}
    
    def process_journal_list(self, journal_list_file: str, max_workers: int = 3, journal_slice: str = ''):
        """
        处理期刊名单（优化版 - 无人值守模式）
        
        Args:
            journal_list_file: 期刊名单文件
            max_workers: 并发下载线程数（默认3，建议1-5）
            journal_slice: 期刊分片（格式 "start:end"，如 "0:263"），空字符串表示处理全部
        """
        print(f"\n🚀 开始批量下载任务（无人值守模式）")
        
        try:
            with open(journal_list_file, 'r', encoding='utf-8') as f:
                all_journals = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"❌ 读取期刊名单失败: {e}")
            return
        
        # 支持分片（用于多进程并行，每个进程处理一段期刊列表）
        journals = all_journals
        slice_info = ""
        if journal_slice and ':' in journal_slice:
            try:
                parts = journal_slice.split(':', 1)
                start = int(parts[0]) if parts[0] else 0
                end = int(parts[1]) if parts[1] else len(all_journals)
                journals = all_journals[start:end]
                slice_info = f" [分片 {start}:{end}，共{len(all_journals)}本中的{len(journals)}本]"
                print(f"📌 并行分片模式: 处理第 {start+1}-{end} 本期刊（共 {len(journals)} 本）")
            except ValueError:
                print(f"⚠️  JOURNAL_SLICE 格式错误（应为 'start:end'，如 '0:263'），将处理全部期刊")
        
        years = list(range(2025, 2005, -1))  # 2025到2006年（倒序）
        
        # 计算总任务数
        self.total_journal_years = len(journals) * len(years)
        
        print(f"📚 共找到 {len(journals)} 本期刊{slice_info}")
        print(f"📅 下载年份: {', '.join(map(str, years))}")
        print(f"📊 总任务数: {self.total_journal_years} 个期刊年份")
        print(f"⚡ 并发线程数: {max_workers} (每期刊内部并发)")
        print(f"♾️  无人值守：所有任务将连续执行，遇错自动跳过\n")
        
        # 检查已完成的任务
        completed = len(self.global_stats['completed_journal_years'])
        if completed > 0:
            progress_pct = (completed / self.total_journal_years) * 100
            print(f"✅ 检测到历史进度: {completed}/{self.total_journal_years} ({progress_pct:.1f}%)")
            print(f"   将自动跳过已完成的期刊年份\n")
        
        # 初始化本次会话（🔥 添加锁保护）
        with self.global_stats_lock:
            self.global_stats['current_session_start'] = datetime.now()
            self.global_stats['total_sessions'] += 1
            self._save_global_stats()
        
        start_time = time.time()
        failed_tasks = []  # 记录失败的任务
        
        try:
            for year in years:
                print(f"\n{'='*80}")
                print(f"📆 开始下载 {year} 年的所有期刊")
                print(f"{'='*80}\n")
                
                for idx, journal_name in enumerate(journals, 1):
                    try:
                        print(f"\n[{year}年 第{idx}/{len(journals)}本期刊]", flush=True)
                        print(f"🔍 准备处理: {journal_name}", flush=True)
                        
                        # 调用 process_journal 并检查是否成功
                        success = self.process_journal(journal_name, year)
                        
                        print(f"🔙 process_journal 已返回，结果: {'成功' if success else '失败'}", flush=True)
                        
                        # 🔥 关键改动：只有成功才算完成，失败的留给下次重试
                        if not success:
                            failed_tasks.append(f"{journal_name} ({year}年)")
                            print(f"   ⚠️  期刊处理失败，已记录到失败列表（下次运行将自动重试）", flush=True)
                        
                        if idx < len(journals):
                            print(f"⏸️  等待 {self.delay * 2:.1f} 秒后继续下一个期刊...", flush=True)
                            time.sleep(self.delay * 2)
                        
                        print(f"✅ 准备处理下一个期刊（循环继续）\n", flush=True)
                    
                    except KeyboardInterrupt:
                        # 用户手动中断
                        raise
                    except Exception as e:
                        # 记录错误但继续执行
                        error_msg = f"{journal_name} ({year}年)"
                        failed_tasks.append(error_msg)
                        print(f"\n❌ 处理失败: {error_msg}")
                        print(f"   错误: {str(e)[:100]}")
                        print(f"   ⏭️  自动跳过，继续下一个期刊...\n")
                        
                        # 等待一下再继续
                        time.sleep(5)
                        continue
        
        except KeyboardInterrupt:
            print(f"\n\n⚠️  用户手动中断")
        
        elapsed_time = time.time() - start_time
        
        print(f"\n{'='*80}")
        print(f"🎉 本次会话结束！")
        print(f"⏱️  本次耗时: {elapsed_time/3600:.1f} 小时")
        
        completed = len(self.global_stats['completed_journal_years'])
        progress_pct = (completed / self.total_journal_years) * 100
        remaining = self.total_journal_years - completed
        
        print(f"📊 总体进度: {completed}/{self.total_journal_years} ({progress_pct:.1f}%)")
        print(f"   ✅ 已完成: {completed} 个")
        print(f"   ⏳ 剩余: {remaining} 个")
        
        if failed_tasks:
            print(f"   ⚠️  失败任务: {len(failed_tasks)} 个")
            print(f"\n❌ 失败列表:")
            for task in failed_tasks[:10]:  # 最多显示10个
                print(f"   - {task}")
            if len(failed_tasks) > 10:
                print(f"   ... 还有 {len(failed_tasks)-10} 个（查看日志文件）")
        
        print(f"📁 文件保存在: {self.download_dir.absolute()}")
        print(f"{'='*80}\n")
        
        # 最终全局报告
        self.save_global_report(self.total_journal_years)
        print(f"📊 查看完整统计: {self.global_report_file}\n")
        print(f"💾 进度已保存，下次运行将自动继续\n")
        
        # 保存失败任务列表
        if failed_tasks:
            failed_log = self.download_dir / "_失败任务记录.txt"
            with open(failed_log, 'w', encoding='utf-8') as f:
                f.write(f"失败任务记录 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("="*80 + "\n\n")
                for task in failed_tasks:
                    f.write(f"{task}\n")
            print(f"📝 失败任务已记录: {failed_log.name}\n")


def main():
    """主函数（安全优化版）"""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║       PubMed 期刊论文批量下载器 v4.0 安全优化版             ║
    ║       ✅ XML优先 (AI训练最佳)                               ║
    ║       ✅ 6级智能：PMC XML → PMC PDF → Unpaywall              ║
    ║                  → Sci-Hub(熔断) → Publisher                ║
    ║                  → Selenium(undetected)                     ║
    ║       ✅ PDF完整性校验  ✅ 元数据自动保存                   ║
    ║       ✅ 环境变量管理  ✅ 年份熔断机制                      ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    # 🔐 从环境变量读取配置（安全）
    EMAIL = os.getenv('PUBMED_EMAIL')
    API_KEY = os.getenv('PUBMED_API_KEY')
    
    # 下载策略开关
    ENABLE_UNPAYWALL = os.getenv('ENABLE_UNPAYWALL', 'True').lower() == 'true'
    ENABLE_SCIHUB = os.getenv('ENABLE_SCIHUB', 'True').lower() == 'true'
    ENABLE_SELENIUM = os.getenv('ENABLE_SELENIUM', 'False').lower() == 'true'
    
    # XML优先策略（推荐用于AI训练）
    PREFER_XML = True
    
    # 免疫筛选模式配置
    IMMUNE_FILTER_MODE = os.getenv('IMMUNE_FILTER_MODE', 'False').lower() == 'true'
    IMMUNE_KEYWORDS_FILE = os.getenv('IMMUNE_KEYWORDS_FILE', 'immune_keywords.txt')
    
    JOURNAL_LIST_FILE = "journal_list.txt"
    
    # 并行分片配置（格式："start:end"，如 "0:263"）
    # 由 run_parallel.sh 或手动设置，用于多进程并行下载
    JOURNAL_SLICE = os.getenv('JOURNAL_SLICE', '')
    
    # 验证必需配置
    if not EMAIL:
        print("❌ 错误：未设置 PUBMED_EMAIL 环境变量")
        print("💡 提示：请编辑 .env 文件并设置 PUBMED_EMAIL=your_email@example.com")
        return
    
    if not os.path.exists(JOURNAL_LIST_FILE):
        print(f"❌ 错误：找不到期刊名单文件 {JOURNAL_LIST_FILE}")
        return
    
    # 显示配置
    print(f"\n📋 当前配置:")
    print(f"   📧 邮箱: {EMAIL}")
    print(f"   🔑 API密钥: {'✅ 已配置' if API_KEY else '❌ 未配置'}")
    print(f"   📄 XML优先: {'✅ 已启用 (推荐用于AI训练)' if PREFER_XML else '❌ 已禁用'}")
    print(f"   🔓 Unpaywall: {'✅ 已启用' if ENABLE_UNPAYWALL else '❌ 已禁用'}")
    print(f"   🌐 Sci-Hub: {'✅ 已启用 (带年份熔断)' if ENABLE_SCIHUB else '❌ 已禁用'}")
    print(f"   🌐 Selenium: {'✅ 已启用 (undetected)' if ENABLE_SELENIUM else '❌ 已禁用'}")
    
    if IMMUNE_FILTER_MODE:
        print(f"   🔬 下载模式: ✅ 免疫筛选模式（指标+白名单期刊）")
        print(f"   📝 关键词文件: {IMMUNE_KEYWORDS_FILE}")
        print(f"   📂 保存路径: downloaded_index/{{关键词}}/{{期刊--年份--期次}}/")
    else:
        print(f"   📦 下载模式: 全量模式（下载白名单期刊全部文章）")
        print(f"   📂 保存路径: downloaded_papers/{{期刊--年份--期次}}/")
    
    print(f"\n🔐 安全特性:")
    print(f"   ✅ API密钥通过环境变量管理（不暴露在代码中）")
    print(f"   ✅ PDF完整性校验（自动移除损坏文件）")
    print(f"   ✅ 年份熔断机制（跳过Sci-Hub无效请求）")
    print(f"   ✅ 元数据自动保存（JSON格式，用于RAG）")
    print()
    
    # 创建下载器
    downloader = PubMedDownloader(
        email=EMAIL, 
        api_key=API_KEY,
        enable_unpaywall=ENABLE_UNPAYWALL,
        enable_scihub=ENABLE_SCIHUB,
        enable_selenium=ENABLE_SELENIUM,
        prefer_xml=PREFER_XML,
        immune_filter_mode=IMMUNE_FILTER_MODE,
        immune_keywords_file=IMMUNE_KEYWORDS_FILE
    )
    
    # 免疫筛选模式：打印已加载的关键词摘要
    if IMMUNE_FILTER_MODE and downloader.immune_keywords:
        groups = {}
        for kw, grp in downloader.immune_keywords:
            groups.setdefault(grp, []).append(kw)
        print(f"\n🔬 已加载关键词 ({len(downloader.immune_keywords)} 个，{len(groups)} 个分组):")
        for grp_name, kw_list in list(groups.items())[:5]:  # 最多显示5组预览
            preview = "、".join(kw_list[:4])
            more = f"...共{len(kw_list)}个" if len(kw_list) > 4 else ""
            print(f"   [{grp_name}] {preview}{more}")
        if len(groups) > 5:
            print(f"   ... 还有 {len(groups)-5} 个分组（查看 {IMMUNE_KEYWORDS_FILE}）")
        print()
    
    # 开始下载
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
