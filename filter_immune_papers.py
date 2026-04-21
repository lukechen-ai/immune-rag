#!/usr/bin/env python3
"""
免疫主题文献筛选器 v2.0
根据标题、摘要、期刊筛选免疫相关文献
增强版：扩充免疫术语库 + 物理文件归类
"""

import json
import csv
import re
import shutil
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict

# ================== 配置区域 ==================

# 免疫相关关键词（大幅扩充）
IMMUNE_KEYWORDS = {
    "核心免疫术语": [
        r"\bimmun\w*",  # immune, immunity, immunology, immunotherapy, immunosuppression
        r"\bantibod\w*",  # antibody, antibodies
        r"\bantigen\w*",  # antigen, antigenic, antigenicity
        r"\bvaccin\w*",  # vaccine, vaccination, vaccinated
        r"\bT[- ]cell\w*",
        r"\bB[- ]cell\w*",
        r"\bNK[- ]cell\w*",
        r"\bimmunogenic\w*",
        r"\bimmunomodulat\w*",
        r"\bimmunosurveillance",
        r"\bimmunosenescence",
        r"\bimmunophenotyp\w*",
    ],
    "炎症与细胞因子": [
        r"\binflamma\w*",  # inflammation, inflammatory, inflammasome
        r"\bcytokine\w*",
        r"\bchemokine\w*",
        r"\binterleukin",
        r"\bIL-\d+\w*",  # IL-1, IL-6, IL-10, IL-1β
        r"\bTNF[- ]?α?",
        r"\bTNF[- ]?alpha",
        r"\binterferon\w*",
        r"\bIFN[- ]?[αβγ]?",
        r"\bTGF[- ]?β?",
        r"\bGM-CSF",
        r"\bG-CSF",
        r"\bM-CSF",
        r"\bCCL\d+",  # 趋化因子
        r"\bCXCL\d+",
        r"\binflammasome\w*",
        r"\bNLRP\d+",  # NLRP3等炎症小体
        r"\bpyroptosis",
        r"\bnecroptosis",
    ],
    "免疫细胞": [
        r"\bmacrophage\w*",
        r"\bmonocyte\w*",
        r"\bneutrophil\w*",
        r"\beosinophil\w*",
        r"\bbasophil\w*",
        r"\blymphocyte\w*",
        r"\bdendritic cell\w*",
        r"\bmast cell\w*",
        r"\bT[- ]?helper",
        r"\bTh\d+",  # Th1, Th2, Th17
        r"\bTreg",  # 调节性T细胞
        r"\bregulatoryT",
        r"\bCD4\+",
        r"\bCD8\+",
        r"\bcytotoxic T",
        r"\bplasma cell\w*",
        r"\bmemory B cell",
        r"\bfollicular helper",
        r"\bM1 macrophage",
        r"\bM2 macrophage",
    ],
    "免疫分子与受体": [
        r"\bIgG\b",
        r"\bIgE\b",
        r"\bIgM\b",
        r"\bIgA\b",
        r"\bIgD\b",
        r"\bMHC[- ]?\w*",
        r"\bHLA[- ]\w+",
        r"\bCD\d+\w*",  # CD4, CD8, CD19, CD20, CD25, CD28等
        r"\bTLR[- ]?\d*",  # Toll-like receptor
        r"\bNOD[- ]?like",
        r"\bRIG-I",
        r"\bPD-1",
        r"\bPD-L1",
        r"\bCTLA-4",
        r"\bTCR\b",  # T cell receptor
        r"\bBCR\b",  # B cell receptor
        r"\bFc receptor",
        r"\bFcγR",
        r"\bFcεR",
        r"\bcomplement\w*",
        r"\bC3\w*",
        r"\bC5\w*",
        r"\bperforin",
        r"\bgranzyme\w*",
    ],
    "免疫疾病": [
        r"\bautoimmun\w*",
        r"\ballerg\w*",  # allergy, allergic, allergen
        r"\basthma\w*",
        r"\batop\w*",  # atopic, atopy
        r"\blupus",
        r"\bSLE\b",  # Systemic lupus erythematosus
        r"\brheumatoid\w*",
        r"\bRA\b",  # Rheumatoid arthritis (需谨慎，可能误匹配)
        r"\bdermatitis",
        r"\beczema",
        r"\bpsoriasis",
        r"\bCrohn[''']?s",
        r"\bulcerative colitis",
        r"\bIBD\b",  # Inflammatory bowel disease
        r"\bmultiple sclerosis",
        r"\bMS\b",  # 需结合上下文
        r"\btype 1 diabetes",
        r"\bT1D\b",
        r"\bGraves[''']? disease",
        r"\bHashimoto",
        r"\bceliac disease",
        r"\bmyasthenia gravis",
        r"\bGuilain[- ]Barré",
        r"\bscleroderma",
        r"\bvitiligo",
        r"\balopecia areata",
        r"\banaphyla\w*",
    ],
    "免疫过程与机制": [
        r"\bphagocytos\w*",
        r"\bopsoniza\w*",
        r"\bantigen presentation",
        r"\bantigen[- ]presenting",
        r"\bcross[- ]presentation",
        r"\bclonal expansion",
        r"\bclonal selection",
        r"\bimmune tolerance",
        r"\bcentral tolerance",
        r"\bperipheral tolerance",
        r"\bcostimulat\w*",
        r"\bimmune checkpoint\w*",
        r"\bimmune evasion",
        r"\bimmune escape",
        r"\bimmunological memory",
        r"\baffinity maturation",
        r"\bsomatic hypermutation",
        r"\bclass switching",
        r"\bisotype switching",
        r"\bgerminal center",
    ],
    "免疫治疗": [
        r"\bimmunotherap\w*",
        r"\bCAR[- ]?T",
        r"\bchimeric antigen",
        r"\bcheckpoint inhibitor",
        r"\bimmune checkpoint",
        r"\banti[- ]?PD-?1",
        r"\banti[- ]?PD-?L1",
        r"\banti[- ]?CTLA-?4",
        r"\bmonoclonal antibod\w*",
        r"\bcytokine therapy",
        r"\badoptive transfer",
        r"\bcancer immunotherapy",
        r"\btumor immunology",
        r"\bcancer vaccine",
        r"\bimmune adjuvant",
        r"\bvaccine adjuvant",
    ],
    "先天免疫": [
        r"\binnate immun\w*",
        r"\bpattern recognition",
        r"\bPAMP\w*",  # Pathogen-associated molecular patterns
        r"\bDAMP\w*",  # Damage-associated molecular patterns
        r"\bnatural killer",
        r"\bNK cell",
        r"\bcomplement cascade",
        r"\bcomplement system",
        r"\binflammasome",
        r"\btype I interferon",
    ],
    "适应性免疫": [
        r"\badaptive immun\w*",
        r"\bacquired immun\w*",
        r"\bhumoral immun\w*",
        r"\bcell[- ]mediated immun\w*",
        r"\bclonal\w*",
        r"\bimmunological synapse",
    ],
    "黏膜免疫": [
        r"\bmucosal immun\w*",
        r"\bsecretory IgA",
        r"\bsIgA\b",
        r"\bgut[- ]associated lymphoid",
        r"\bGALT\b",
        r"\bPeyer[''']?s patch\w*",
        r"\bM cell\w*",  # 微褶皱细胞
    ],
    "免疫组学": [
        r"\bimmunogen\w*",
        r"\bimmunoproteom\w*",
        r"\bimmunopeptid\w*",
        r"\bepitope\w*",
        r"\bMHC peptid\w*",
        r"\bHLA typing",
        r"\bT cell repertoire",
        r"\bBCR repertoire",
        r"\bTCR repertoire",
        r"\bimmune repertoire",
    ],
}

# 核心免疫期刊（从 journal_list.txt 提取）
CORE_IMMUNE_JOURNALS = [
    "allergy",  # 使用小写便于匹配
    "immunology",
    "immunological",
    "immunity",
    "immune",
]

# 部分相关的期刊关键词
PARTIAL_IMMUNE_JOURNALS = [
    "infectious disease",
    "transplant",
    "rheumatic",
    "respiratory",
    "asthma",
]


class ImmuneLiteratureClassifier:
    """免疫文献分类器 - 增强版"""
    
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        
        # 已下载文献结果
        self.downloaded_results = {
            "高度相关": [],
            "中度相关": [],
            "低度相关": [],
            "不相关": [],
        }
        
        # 失败清单结果
        self.failed_results = {
            "高度相关": [],
            "中度相关": [],
            "低度相关": [],
            "不相关": [],
        }
        
        self.stats = {
            "downloaded": defaultdict(int),
            "failed": defaultdict(int),
        }
        
    def calculate_immune_score(self, text: str, journal: str) -> Tuple[int, Dict]:
        """计算免疫相关度分数"""
        text_lower = text.lower()
        journal_lower = journal.lower()
        score = 0
        matched_categories = defaultdict(list)
        
        # 期刊加分
        for core_journal in CORE_IMMUNE_JOURNALS:
            if core_journal in journal_lower:
                score += 100
                matched_categories["期刊权重"].append(f"核心免疫期刊: {journal}")
                break
        else:
            for partial_journal in PARTIAL_IMMUNE_JOURNALS:
                if partial_journal in journal_lower:
                    score += 30
                    matched_categories["期刊权重"].append(f"部分相关期刊: {journal}")
                    break
        
        # 关键词匹配
        for category, patterns in IMMUNE_KEYWORDS.items():
            category_matches = []
            for pattern in patterns:
                matches = re.findall(pattern, text_lower, re.IGNORECASE)
                if matches:
                    category_matches.extend(matches)
            
            if category_matches:
                # 每个类别首次匹配得10分，后续每次得2分
                unique_matches = list(set(category_matches))
                category_score = 10 + len(category_matches) * 2
                score += category_score
                matched_categories[category] = unique_matches[:10]  # 最多保存10个示例
        
        return score, dict(matched_categories)
    
    def classify_paper(self, metadata: Dict) -> Tuple[str, int, Dict]:
        """分类单篇文献"""
        title = metadata.get("title", "")
        abstract = metadata.get("abstract", "")
        journal = metadata.get("journal", "")
        
        # 合并文本
        full_text = f"{title} {abstract}"
        
        # 计算分数
        score, matches = self.calculate_immune_score(full_text, journal)
        
        # 分类
        if score >= 100:
            category = "高度相关"
        elif score >= 30:
            category = "中度相关"
        elif score >= 10:
            category = "低度相关"
        else:
            category = "不相关"
        
        return category, score, matches
    
    def scan_downloaded_papers(self):
        """扫描所有已下载的文献"""
        metadata_files = list(self.base_dir.glob("**/*_metadata.json"))
        
        print(f"📊 找到 {len(metadata_files)} 个已下载文献的元数据文件")
        print(f"🔍 开始免疫主题筛选...\n")
        
        for i, meta_file in enumerate(metadata_files, 1):
            try:
                with open(meta_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                
                pmid = metadata.get("pmid", "unknown")
                category, score, matches = self.classify_paper(metadata)
                
                # 获取文本文件路径
                txt_file = meta_file.with_suffix('.txt')
                txt_exists = txt_file.exists()
                
                # 保存结果
                result = {
                    "pmid": pmid,
                    "title": metadata.get("title", ""),
                    "journal": metadata.get("journal", ""),
                    "year": metadata.get("year", ""),
                    "score": score,
                    "matched_keywords": matches,
                    "folder_name": meta_file.parent.name,
                    "doi": metadata.get("doi", ""),
                    "fulltext_source": metadata.get("fulltext_source", ""),
                    "txt_file": str(txt_file.relative_to(self.base_dir)) if txt_exists else "",
                    "metadata_file": str(meta_file.relative_to(self.base_dir)),
                }
                
                self.downloaded_results[category].append(result)
                self.stats["downloaded"][category] += 1
                
                # 进度显示
                if i % 100 == 0:
                    print(f"  已处理: {i}/{len(metadata_files)} ({i/len(metadata_files)*100:.1f}%)")
                
            except Exception as e:
                print(f"⚠️  处理失败 {meta_file.name}: {e}")
                continue
        
        print(f"\n✅ 已下载文献扫描完成！共处理 {len(metadata_files)} 篇\n")
    
    def scan_failed_list(self):
        """扫描失败清单中的文献"""
        failed_csvs = list(self.base_dir.glob("_失败清单_*.csv"))
        
        print(f"📋 找到 {len(failed_csvs)} 个失败清单文件")
        
        for csv_file in failed_csvs:
            journal_name = csv_file.stem.replace("_失败清单_", "")
            print(f"  处理: {csv_file.name}")
            
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 仅基于标题分类（失败清单没有摘要）
                    pseudo_metadata = {
                        "title": row.get("title", ""),
                        "abstract": "",
                        "journal": row.get("journal", ""),
                    }
                    
                    category, score, matches = self.classify_paper(pseudo_metadata)
                    
                    result = {
                        "pmid": row.get("pmid", ""),
                        "title": row.get("title", ""),
                        "journal": row.get("journal", ""),
                        "year": row.get("year", ""),
                        "score": score,
                        "matched_keywords": matches,
                        "doi": row.get("doi", ""),
                        "pubmed_url": row.get("pubmed_url", ""),
                        "source_list": journal_name,
                    }
                    
                    self.failed_results[category].append(result)
                    self.stats["failed"][category] += 1
        
        print(f"✅ 失败清单扫描完成！\n")
    
    def organize_files_by_category(self, output_dir: Path):
        """按分类物理移动/复制文件"""
        print(f"\n📁 开始组织文件到分类文件夹...\n")
        
        # 为已下载文献创建分类文件夹
        for category in ["高度相关", "中度相关", "低度相关"]:
            category_dir = output_dir / "已下载文献" / category
            category_dir.mkdir(parents=True, exist_ok=True)
            
            papers = self.downloaded_results[category]
            if not papers:
                continue
            
            print(f"📂 处理「{category}」: {len(papers)} 篇")
            
            for paper in papers:
                try:
                    # 创建子文件夹（使用 PMID_部分标题）
                    safe_title = "".join(c for c in paper['title'][:50] if c.isalnum() or c in (' ', '-', '_')).strip()
                    paper_folder = category_dir / f"{paper['pmid']}_{safe_title}"
                    paper_folder.mkdir(exist_ok=True)
                    
                    # 复制元数据文件
                    metadata_src = self.base_dir / paper['metadata_file']
                    if metadata_src.exists():
                        shutil.copy2(metadata_src, paper_folder / metadata_src.name)
                    
                    # 复制文本文件
                    if paper['txt_file']:
                        txt_src = self.base_dir / paper['txt_file']
                        if txt_src.exists():
                            shutil.copy2(txt_src, paper_folder / txt_src.name)
                    
                except Exception as e:
                    print(f"  ⚠️  复制失败 {paper['pmid']}: {e}")
            
            print(f"  ✅ 完成「{category}」文件组织\n")
        
        # 为失败清单创建分类CSV（不涉及文件复制）
        print(f"📋 生成失败清单分类文件...\n")
    
    def export_results(self, output_dir: Path):
        """导出分类结果"""
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True)
        
        # === 1. 导出已下载文献的CSV清单 ===
        downloaded_csv_dir = output_dir / "已下载文献" / "CSV清单"
        downloaded_csv_dir.mkdir(parents=True, exist_ok=True)
        
        for category, papers in self.downloaded_results.items():
            if not papers:
                continue
            
            csv_file = downloaded_csv_dir / f"已下载_{category}.csv"
            
            with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
                fieldnames = ['pmid', 'title', 'journal', 'year', 'score', 
                             'matched_keywords', 'folder_name', 'doi', 'fulltext_source', 'txt_file']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                # 按分数排序
                sorted_papers = sorted(papers, key=lambda x: x['score'], reverse=True)
                
                for paper in sorted_papers:
                    # 创建副本并格式化匹配关键词
                    row = paper.copy()
                    row['matched_keywords'] = "; ".join([
                        f"{cat}: {', '.join(str(kw) for kw in kws[:5])}"
                        for cat, kws in paper['matched_keywords'].items()
                    ])
                    writer.writerow(row)
            
            print(f"✅ 已导出: {csv_file.name} ({len(papers)} 篇)")
        
        # === 2. 导出失败清单的CSV ===
        failed_csv_dir = output_dir / "失败清单" / "CSV清单"
        failed_csv_dir.mkdir(parents=True, exist_ok=True)
        
        for category, papers in self.failed_results.items():
            if not papers:
                continue
            
            csv_file = failed_csv_dir / f"失败清单_{category}.csv"
            
            with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
                fieldnames = ['pmid', 'title', 'journal', 'year', 'score', 
                             'matched_keywords', 'doi', 'pubmed_url', 'source_list']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                sorted_papers = sorted(papers, key=lambda x: x['score'], reverse=True)
                
                for paper in sorted_papers:
                    # 创建副本并格式化匹配关键词
                    row = paper.copy()
                    row['matched_keywords'] = "; ".join([
                        f"{cat}: {', '.join(str(kw) for kw in kws[:5])}"
                        for cat, kws in paper['matched_keywords'].items()
                    ])
                    writer.writerow(row)
            
            print(f"✅ 已导出: {csv_file.name} ({len(papers)} 篇)")
        
        # === 3. 生成统计报告 ===
        self._generate_report(output_dir)
        
        # === 4. 组织文件到分类文件夹 ===
        self.organize_files_by_category(output_dir)
    
    def _generate_report(self, output_dir: Path):
        """生成统计报告"""
        report_file = output_dir / "免疫筛选_统计报告.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("=" * 100 + "\n")
            f.write(" " * 35 + "免疫相关文献筛选报告\n")
            f.write("=" * 100 + "\n\n")
            
            # 已下载文献统计
            f.write("【一、已下载文献】\n")
            f.write("-" * 100 + "\n\n")
            total_downloaded = sum(self.stats["downloaded"].values())
            
            for category in ["高度相关", "中度相关", "低度相关", "不相关"]:
                count = self.stats["downloaded"][category]
                percentage = count / total_downloaded * 100 if total_downloaded > 0 else 0
                bar = "█" * int(percentage / 2)
                f.write(f"  {category:10s}: {count:5d} 篇 ({percentage:5.1f}%)  {bar}\n")
            
            f.write(f"\n  {'总计':10s}: {total_downloaded:5d} 篇\n\n")
            
            # 失败清单统计
            f.write("【二、失败清单】\n")
            f.write("-" * 100 + "\n\n")
            total_failed = sum(self.stats["failed"].values())
            
            for category in ["高度相关", "中度相关", "低度相关", "不相关"]:
                count = self.stats["failed"][category]
                percentage = count / total_failed * 100 if total_failed > 0 else 0
                bar = "█" * int(percentage / 2)
                f.write(f"  {category:10s}: {count:5d} 篇 ({percentage:5.1f}%)  {bar}\n")
            
            f.write(f"\n  {'总计':10s}: {total_failed:5d} 篇\n\n")
            
            # 综合统计
            f.write("【三、综合统计与建议】\n")
            f.write("-" * 100 + "\n\n")
            total_high = self.stats["downloaded"]["高度相关"] + self.stats["failed"]["高度相关"]
            total_medium = self.stats["downloaded"]["中度相关"] + self.stats["failed"]["中度相关"]
            total_all = total_downloaded + total_failed
            
            f.write(f"  ⭐ 高度相关免疫文献: {total_high} 篇\n")
            f.write(f"     - 已下载: {self.stats['downloaded']['高度相关']} 篇\n")
            f.write(f"     - 下载失败: {self.stats['failed']['高度相关']} 篇\n\n")
            
            f.write(f"  🔶 中度相关免疫文献: {total_medium} 篇\n")
            f.write(f"     - 已下载: {self.stats['downloaded']['中度相关']} 篇\n")
            f.write(f"     - 下载失败: {self.stats['failed']['中度相关']} 篇\n\n")
            
            f.write(f"  📊 免疫相关总计: {total_high + total_medium} 篇\n")
            f.write(f"  📄 文献总数: {total_all} 篇\n\n")
            
            immune_rate = (total_high + total_medium) / total_all * 100 if total_all > 0 else 0
            f.write(f"  ✅ 免疫相关率: {immune_rate:.1f}%\n\n")
            
            # 训练建议
            f.write("【四、模型训练建议】\n")
            f.write("-" * 100 + "\n\n")
            f.write(f"  💎 核心训练集（高度相关）:\n")
            f.write(f"     使用 {self.stats['downloaded']['高度相关']} 篇已下载的「高度相关」文献\n")
            f.write(f"     位置: 免疫筛选结果/已下载文献/高度相关/\n\n")
            
            f.write(f"  🔸 扩展训练集（中度相关）:\n")
            f.write(f"     使用 {self.stats['downloaded']['中度相关']} 篇已下载的「中度相关」文献\n")
            f.write(f"     位置: 免疫筛选结果/已下载文献/中度相关/\n\n")
            
            if self.stats['failed']['高度相关'] > 0:
                f.write(f"  🔄 建议重新下载:\n")
                f.write(f"     失败清单中有 {self.stats['failed']['高度相关']} 篇「高度相关」文献\n")
                f.write(f"     清单位置: 免疫筛选结果/失败清单/CSV清单/失败清单_高度相关.csv\n\n")
            
            # 关键词匹配统计
            f.write("【五、关键词类别统计】\n")
            f.write("-" * 100 + "\n\n")
            f.write(f"  本次筛选使用了 {len(IMMUNE_KEYWORDS)} 大类免疫关键词:\n")
            for i, category in enumerate(IMMUNE_KEYWORDS.keys(), 1):
                f.write(f"    {i}. {category} ({len(IMMUNE_KEYWORDS[category])} 个模式)\n")
            f.write("\n")
        
        print(f"\n📊 统计报告已生成: {report_file.name}")


def main():
    """主函数"""
    base_dir = Path(__file__).parent / "downloaded_papers"
    output_dir = Path(__file__).parent / "免疫筛选结果"
    
    print("=" * 100)
    print(" " * 35 + "🧬 免疫相关文献筛选器 v2.0")
    print("=" * 100 + "\n")
    print("✨ 新特性:")
    print("  - 扩充免疫术语库（10大类，200+关键词模式）")
    print("  - 已下载文献与失败清单分类处理")
    print("  - 自动组织文件到分类文件夹\n")
    print("=" * 100 + "\n")
    
    classifier = ImmuneLiteratureClassifier(base_dir)
    
    # 扫描已下载文献
    print("【第一步】扫描已下载文献")
    print("-" * 100)
    classifier.scan_downloaded_papers()
    
    # 扫描失败清单
    print("【第二步】扫描失败清单")
    print("-" * 100)
    classifier.scan_failed_list()
    
    # 打印统计
    print("【第三步】分类统计")
    print("-" * 100)
    print("\n📊 已下载文献分类：")
    for category, count in classifier.stats["downloaded"].items():
        print(f"  {category:10s}: {count:5d} 篇")
    
    print("\n📋 失败清单分类：")
    for category, count in classifier.stats["failed"].items():
        print(f"  {category:10s}: {count:5d} 篇")
    
    # 导出结果
    print(f"\n【第四步】导出结果")
    print("-" * 100)
    print(f"💾 输出目录: {output_dir}\n")
    classifier.export_results(output_dir)
    
    print("\n" + "=" * 100)
    print(" " * 40 + "✅ 筛选完成！")
    print("=" * 100 + "\n")
    
    print("📂 结果目录结构:")
    print("  免疫筛选结果/")
    print("  ├── 已下载文献/")
    print("  │   ├── 高度相关/        # 按 PMID 分文件夹存储")
    print("  │   ├── 中度相关/")
    print("  │   ├── 低度相关/")
    print("  │   └── CSV清单/         # 汇总CSV")
    print("  ├── 失败清单/")
    print("  │   └── CSV清单/         # 按相关度分类的失败文献清单")
    print("  └── 免疫筛选_统计报告.txt")
    print()


if __name__ == "__main__":
    main()
