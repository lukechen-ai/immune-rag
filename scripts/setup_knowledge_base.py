#!/usr/bin/env python3
"""
IT-X 免疫学文献知识库构建脚本

功能：
- 通过 Dify API 创建免疫学文献知识库
- 批量上传免疫学文献（PDF/TXT格式）
- 配置最优的文本分割和向量化参数
- 输出知识库ID供工作流配置使用

使用方法：
    pip install requests tqdm
    python scripts/setup_knowledge_base.py \
        --api-base https://your-dify-instance.com \
        --api-key your-api-key \
        --docs-dir ./knowledge_base/papers
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests
from tqdm import tqdm


# ─── 配置常量 ───────────────────────────────────────────────────────────────

DATASET_NAME = "IT-X 免疫学文献库"
DATASET_DESCRIPTION = (
    "收录免疫学领域权威文献，包括：基础免疫学机制、自身免疫性疾病、"
    "肿瘤免疫、移植免疫、疫苗与免疫治疗等方向的研究论文与综述。"
)

# 支持的文档格式
SUPPORTED_EXTENSIONS = {".pdf", ".txt", ".md", ".docx", ".csv"}

# 文本分割配置（针对医学文献优化）
SEGMENTATION_CONFIG = {
    "mode": "custom",
    "rules": {
        "pre_processing_rules": [
            {"id": "remove_extra_spaces", "enabled": True},
            {"id": "remove_urls_emails", "enabled": False},  # 保留DOI链接
        ],
        "segmentation": {
            # 医学文献段落通常较长，设置较大的chunk size
            "separator": "\n\n",
            "max_tokens": 1000,    # 每块最大token数
            "chunk_overlap": 150,  # 重叠token，保持上下文连贯
        },
    },
}

# 检索配置
RETRIEVAL_CONFIG = {
    "retrieval_model": {
        "search_method": "hybrid_search",  # 混合检索（语义+关键词）
        "reranking_enable": True,
        "reranking_mode": "reranking_model",
        "top_k": 8,
        "score_threshold_enabled": True,
        "score_threshold": 0.3,
    }
}

# 推荐的免疫学文献分类标签
IMMUNOLOGY_CATEGORIES = [
    "基础免疫学",
    "先天免疫",
    "适应性免疫",
    "T细胞生物学",
    "B细胞与抗体",
    "细胞因子与信号转导",
    "自身免疫性疾病",
    "肿瘤免疫学",
    "移植免疫",
    "疫苗学",
    "免疫治疗",
    "过敏与超敏反应",
    "感染免疫",
    "免疫缺陷",
    "临床免疫学",
]


# ─── Dify API 客户端 ─────────────────────────────────────────────────────────

class DifyKnowledgeBaseClient:
    """Dify 知识库 API 客户端"""

    def __init__(self, api_base: str, api_key: str):
        self.api_base = api_base.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
        })

    def _url(self, path: str) -> str:
        return f"{self.api_base}/v1{path}"

    def create_dataset(self) -> dict:
        """创建免疫学文献知识库"""
        payload = {
            "name": DATASET_NAME,
            "description": DATASET_DESCRIPTION,
            "permission": "only_me",
            "indexing_technique": "high_quality",  # 高质量向量索引
            "embedding_model": "text-embedding-3-large",
            "embedding_model_provider": "openai",
            "retrieval_model": RETRIEVAL_CONFIG["retrieval_model"],
        }

        resp = self.session.post(self._url("/datasets"), json=payload)
        resp.raise_for_status()
        return resp.json()

    def get_datasets(self) -> list:
        """获取已有知识库列表"""
        resp = self.session.get(self._url("/datasets"), params={"limit": 100})
        resp.raise_for_status()
        return resp.json().get("data", [])

    def upload_document_by_file(
        self,
        dataset_id: str,
        file_path: Path,
        doc_metadata: dict | None = None,
    ) -> dict:
        """上传单个文献文件到知识库"""
        process_rule = {
            "mode": SEGMENTATION_CONFIG["mode"],
            "rules": SEGMENTATION_CONFIG["rules"],
        }

        data = {
            "indexing_technique": "high_quality",
            "process_rule": json.dumps(process_rule),
        }

        if doc_metadata:
            data["doc_metadata"] = json.dumps(doc_metadata)

        with open(file_path, "rb") as f:
            files = {"file": (file_path.name, f, _mime_type(file_path))}
            resp = self.session.post(
                self._url(f"/datasets/{dataset_id}/document/create-by-file"),
                data=data,
                files=files,
            )

        resp.raise_for_status()
        return resp.json()

    def upload_document_by_text(
        self,
        dataset_id: str,
        name: str,
        text: str,
        doc_metadata: dict | None = None,
    ) -> dict:
        """通过文本内容直接创建知识库文档（适用于结构化文献摘要）"""
        payload = {
            "name": name,
            "text": text,
            "indexing_technique": "high_quality",
            "process_rule": SEGMENTATION_CONFIG,
        }
        if doc_metadata:
            payload["doc_metadata"] = doc_metadata

        resp = self.session.post(
            self._url(f"/datasets/{dataset_id}/document/create-by-text"),
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def get_document_status(self, dataset_id: str, batch_id: str) -> dict:
        """查询文档索引状态"""
        resp = self.session.get(
            self._url(f"/datasets/{dataset_id}/documents/{batch_id}/indexing-status")
        )
        resp.raise_for_status()
        return resp.json()

    def wait_for_indexing(
        self, dataset_id: str, batch_id: str, timeout: int = 300
    ) -> bool:
        """等待文档完成向量索引"""
        start = time.time()
        while time.time() - start < timeout:
            status = self.get_document_status(dataset_id, batch_id)
            docs = status.get("data", [])
            if not docs:
                return False
            all_done = all(
                d.get("indexing_status") in ("completed", "error")
                for d in docs
            )
            if all_done:
                return all(d.get("indexing_status") == "completed" for d in docs)
            time.sleep(5)
        return False


# ─── 辅助函数 ────────────────────────────────────────────────────────────────

def _mime_type(path: Path) -> str:
    mapping = {
        ".pdf": "application/pdf",
        ".txt": "text/plain",
        ".md": "text/markdown",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".csv": "text/csv",
    }
    return mapping.get(path.suffix.lower(), "application/octet-stream")


def collect_documents(docs_dir: Path) -> list[Path]:
    """递归收集指定目录下所有支持的文献文件"""
    files = []
    for ext in SUPPORTED_EXTENSIONS:
        files.extend(docs_dir.rglob(f"*{ext}"))
    return sorted(files)


def infer_category(filename: str) -> str:
    """根据文件名推断文献分类（可根据实际命名规范调整）"""
    name_lower = filename.lower()
    category_keywords = {
        "tumor": "肿瘤免疫学",
        "cancer": "肿瘤免疫学",
        "pd-1": "肿瘤免疫学",
        "pd-l1": "肿瘤免疫学",
        "checkpoint": "肿瘤免疫学",
        "autoimmun": "自身免疫性疾病",
        "lupus": "自身免疫性疾病",
        "rheumat": "自身免疫性疾病",
        "transplant": "移植免疫",
        "graft": "移植免疫",
        "vaccine": "疫苗学",
        "vaccin": "疫苗学",
        "t cell": "T细胞生物学",
        "tcell": "T细胞生物学",
        "cd4": "T细胞生物学",
        "cd8": "T细胞生物学",
        "b cell": "B细胞与抗体",
        "bcell": "B细胞与抗体",
        "antibod": "B细胞与抗体",
        "cytokine": "细胞因子与信号转导",
        "interferon": "细胞因子与信号转导",
        "interleukin": "细胞因子与信号转导",
        "innate": "先天免疫",
        "toll": "先天免疫",
        "nk cell": "先天免疫",
        "allerg": "过敏与超敏反应",
        "asthma": "过敏与超敏反应",
    }
    for kw, cat in category_keywords.items():
        if kw in name_lower:
            return cat
    return "基础免疫学"


# ─── 示例文献摘要（用于演示/测试） ──────────────────────────────────────────

DEMO_DOCUMENTS = [
    {
        "name": "PD-1/PD-L1免疫检查点抑制剂综述",
        "category": "肿瘤免疫学",
        "text": """
# PD-1/PD-L1 免疫检查点通路与肿瘤免疫治疗

## 摘要
PD-1（程序性死亡受体1）是一种主要在活化T细胞上表达的抑制性受体，其配体PD-L1广泛表达
于肿瘤细胞及免疫细胞。PD-1/PD-L1通路是肿瘤免疫逃逸的核心机制之一。

## 分子机制
PD-1通过其胞质区的免疫受体酪氨酸抑制基序（ITIM）和免疫受体酪氨酸转换基序（ITSM）招募
磷酸酶SHP-2，从而抑制TCR下游信号传导，包括ZAP-70磷酸化、PI3K/Akt和RAS/MAPK通路的激活。

## 肿瘤逃逸机制
1. **PD-L1上调**：肿瘤细胞通过IFN-γ信号、基因组扩增（9p24.1扩增）或致癌信号（EGFR、ALK突变）
   上调PD-L1表达
2. **免疫抑制微环境**：肿瘤微环境（TME）中大量调节性T细胞（Tregs）和髓系抑制细胞（MDSCs）
   进一步抑制抗肿瘤免疫应答
3. **T细胞耗竭**：慢性抗原刺激导致肿瘤浸润淋巴细胞（TILs）出现耗竭表型，高表达多种抑制性受体

## 临床应用
PD-1/PD-L1抑制剂（nivolumab、pembrolizumab、atezolizumab等）已获批用于：
- 非小细胞肺癌（NSCLC）
- 黑色素瘤
- 肾细胞癌
- 膀胱癌
- 霍奇金淋巴瘤等多种肿瘤

## 疗效预测标志物
- PD-L1表达水平（TPS/CPS评分）
- 肿瘤突变负荷（TMB）
- 微卫星不稳定性（MSI-H/dMMR）
- 肿瘤浸润淋巴细胞（TIL）密度

## 耐药机制
原发性耐药与获得性耐药均涉及多种机制，包括JAK1/2突变导致IFN-γ信号缺失、
β2微球蛋白（B2M）突变影响MHC-I类分子表达等。
        """,
    },
    {
        "name": "T细胞激活与免疫突触形成机制",
        "category": "T细胞生物学",
        "text": """
# T细胞激活的分子机制与免疫突触

## T细胞受体（TCR）信号传导
T细胞激活需要双信号：
1. **第一信号**：TCR识别由MHC分子呈递的抗原肽（pMHC复合物）
2. **第二信号**：共刺激分子信号（CD28-CD80/CD86）

## 免疫突触的形成
免疫突触是T细胞与抗原呈递细胞（APC）接触面形成的高度有序的超分子激活簇（SMAC）：

### 中央SMAC（cSMAC）
- TCR/CD3复合物富集区
- PKC-θ、Lck聚集
- 信号转导核心区域

### 外周SMAC（pSMAC）
- LFA-1（αLβ2整合素）与ICAM-1结合
- 形成稳定的细胞黏附环

### 远端SMAC（dSMAC）
- 富含F-actin
- 调控突触动态重塑

## 关键信号分子
- **Lck**：磷酸化CD3ζ链ITAM
- **ZAP-70**：招募至磷酸化ITAM，激活LAT/SLP-76支架蛋白
- **PLCγ1**：水解PIP2产生IP3（→Ca²⁺释放→NFAT激活）和DAG（→PKC-θ→NF-κB）
- **PI3K/Akt**：通过CD28共刺激激活，促进细胞存活和代谢重编程

## 转录因子激活
TCR信号最终激活三大转录因子网络：
1. NFAT（钙调磷酸酶依赖）
2. NF-κB（PKC-θ依赖）
3. AP-1（RAS/MAPK依赖）

这三者协同调控IL-2、IFN-γ等效应细胞因子的转录。
        """,
    },
    {
        "name": "自身免疫性疾病的发病机制综述",
        "category": "自身免疫性疾病",
        "text": """
# 自身免疫性疾病：从遗传易感性到免疫失耐受

## 概述
自身免疫性疾病是机体免疫系统错误攻击自身组织所引起的一类疾病，全球患病率约5-8%，
包括系统性红斑狼疮（SLE）、类风湿关节炎（RA）、多发性硬化（MS）、1型糖尿病等。

## 中枢免疫耐受的破坏

### 胸腺阴性选择缺陷
- AIRE基因（自身免疫调节因子）突变导致多器官自身免疫（APS-1综合征）
- 自身反应性T细胞逃避阴性选择进入外周

### 外周免疫耐受的失衡
1. **调节性T细胞（Treg）功能缺陷**：FOXP3突变（IPEX综合征）
2. **抑制性受体信号异常**：CTLA-4单倍剂量不足与自身免疫相关
3. **分子模拟**：病原体抗原与自身抗原的结构相似性触发交叉反应

## 遗传因素
- **MHC关联**：HLA-DRB1等位基因与RA（SE表位）、SLE等强相关
- **非MHC基因**：PTPN22、STAT4、IRF5等免疫调控基因多态性
- **全基因组关联研究（GWAS）**：已鉴定>100个自身免疫风险位点

## 环境触发因素
- 感染（EBV与SLE/MS；链球菌与风湿热）
- 微生物组失衡（dysbiosis）
- 紫外线（SLE）
- 吸烟（RA、MS）

## 效应机制
### 体液免疫介导
- 抗dsDNA、抗Sm抗体（SLE）→免疫复合物沉积
- 抗CCP抗体（RA）→关节滑膜炎症
- 抗AChR抗体（重症肌无力）→神经肌肉接头阻断

### 细胞免疫介导
- 自身反应性CD4⁺ T细胞驱动的慢性炎症（MS、1型糖尿病）
- Th17/Treg失衡加剧炎症损伤

## 治疗靶点
| 靶点 | 代表药物 | 适应症 |
|------|---------|--------|
| TNF-α | 英夫利昔单抗 | RA、AS、IBD |
| IL-6R | 托珠单抗 | RA、细胞因子风暴 |
| B细胞（CD20） | 利妥昔单抗 | RA、SLE |
| JAK1/3 | 托法替布 | RA、UC |
| IL-17A | 司库奇尤单抗 | AS、银屑病 |
        """,
    },
    {
        "name": "CAR-T细胞疗法：原理与临床应用",
        "category": "免疫治疗",
        "text": """
# 嵌合抗原受体T细胞（CAR-T）疗法

## CAR的结构设计
CAR（嵌合抗原受体）是一种人工合成的融合蛋白，包含：

### 胞外域
- **scFv**（单链可变片段）：来源于单克隆抗体，识别肿瘤相关抗原（TAA）
- 常见靶点：CD19（B细胞恶性肿瘤）、BCMA（多发性骨髓瘤）、CD22、GD2等

### 跨膜域
- 通常来源于CD8α或CD28分子
- 稳定CAR在细胞膜上的表达

### 胞内信号域（决定CAR的"代"）
- **第1代**：仅含CD3ζ链（信号弱，体内扩增差）
- **第2代**：CD3ζ + 一个共刺激域（CD28或4-1BB）
- **第3代**：CD3ζ + 两个共刺激域

## 制备流程
1. 患者外周血单个核细胞（PBMC）采集
2. T细胞分离激活（CD3/CD28抗体+IL-2）
3. 慢病毒或逆转录病毒载体转导CAR基因
4. 体外扩增（10-14天）
5. 质量检验（活率、CAR表达率、残余病毒）
6. 回输前淋巴细胞清除预处理（氟达拉滨+环磷酰胺）

## 临床疗效
| 适应症 | 代表产品 | ORR | 批准年份 |
|--------|---------|-----|---------|
| 复发难治B-ALL | tisagenlecleucel | ~80% | 2017 |
| 复发难治DLBCL | axicabtagene ciloleucel | ~70% | 2017 |
| 多发性骨髓瘤 | idecabtagene vicleucel | ~73% | 2021 |

## 主要不良反应
### 细胞因子释放综合征（CRS）
- 机制：CAR-T大量激活释放IL-6、IFN-γ、TNF-α等
- 分级：1-4级（发热→低血压→器官功能障碍）
- 治疗：托珠单抗（IL-6R拮抗剂）±糖皮质激素

### 免疫效应细胞相关神经毒性综合征（ICANS）
- 机制：血脑屏障通透性增加，细胞因子进入CNS
- 表现：谵妄、失语、脑病
- 治疗：糖皮质激素

## 局限性与改进方向
- **抗原逃逸**：靶向多抗原的"Tandem CAR"或"OR-gate CAR"
- **实体瘤障碍**：TME免疫抑制、肿瘤归巢困难
- **持久性不足**：优化共刺激域（4-1BB优于CD28）
- **通用型CAR-T**：基因编辑（CRISPR）去除TCR和MHC-I，降低GvHD和排斥风险
        """,
    },
]


# ─── 主程序 ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="IT-X 免疫学文献知识库构建工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--api-base",
        default=os.getenv("DIFY_API_BASE", "http://localhost"),
        help="Dify 服务地址（默认：http://localhost）",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("DIFY_API_KEY"),
        required=not os.getenv("DIFY_API_KEY"),
        help="Dify API 密钥（也可通过环境变量 DIFY_API_KEY 设置）",
    )
    parser.add_argument(
        "--docs-dir",
        type=Path,
        default=Path("knowledge_base/papers"),
        help="文献文件目录（默认：./knowledge_base/papers）",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="上传内置演示文献（无需本地文件）",
    )
    parser.add_argument(
        "--dataset-id",
        help="已有知识库ID（跳过创建步骤，直接上传文献）",
    )

    args = parser.parse_args()

    client = DifyKnowledgeBaseClient(args.api_base, args.api_key)

    # 1. 获取或创建知识库
    if args.dataset_id:
        dataset_id = args.dataset_id
        print(f"✅ 使用已有知识库: {dataset_id}")
    else:
        print("📚 正在创建免疫学文献知识库...")
        try:
            dataset = client.create_dataset()
            dataset_id = dataset["id"]
            print(f"✅ 知识库创建成功！")
            print(f"   名称: {dataset['name']}")
            print(f"   ID: {dataset_id}")
        except requests.HTTPError as e:
            # 若知识库已存在，查找并复用
            if e.response.status_code == 409:
                datasets = client.get_datasets()
                existing = next(
                    (d for d in datasets if d["name"] == DATASET_NAME), None
                )
                if existing:
                    dataset_id = existing["id"]
                    print(f"ℹ️ 已有同名知识库，复用: {dataset_id}")
                else:
                    raise
            else:
                raise

    print(f"\n{'='*60}")
    print(f"知识库 ID: {dataset_id}")
    print(f"{'='*60}")

    # 2. 上传演示文献
    if args.demo:
        print(f"\n📄 正在上传 {len(DEMO_DOCUMENTS)} 篇演示文献...")
        for doc in tqdm(DEMO_DOCUMENTS, desc="上传进度"):
            try:
                result = client.upload_document_by_text(
                    dataset_id=dataset_id,
                    name=doc["name"],
                    text=doc["text"],
                    doc_metadata={"category": doc["category"], "source": "demo"},
                )
                batch_id = result.get("document", {}).get("id", "")
                print(f"  ✅ {doc['name']} (ID: {batch_id})")
            except Exception as exc:
                print(f"  ❌ {doc['name']} 上传失败: {exc}", file=sys.stderr)

    # 3. 上传本地文献文件
    if args.docs_dir.exists():
        docs = collect_documents(args.docs_dir)
        if docs:
            print(f"\n📂 发现 {len(docs)} 个文献文件，开始批量上传...")
            success, failed = 0, 0
            for doc_path in tqdm(docs, desc="上传文献"):
                try:
                    category = infer_category(doc_path.name)
                    result = client.upload_document_by_file(
                        dataset_id=dataset_id,
                        file_path=doc_path,
                        doc_metadata={
                            "category": category,
                            "filename": doc_path.name,
                            "source": "local_upload",
                        },
                    )
                    success += 1
                except Exception as exc:
                    print(f"\n  ❌ {doc_path.name}: {exc}", file=sys.stderr)
                    failed += 1

            print(f"\n上传完成：成功 {success}，失败 {failed}")
        else:
            print(f"ℹ️ 目录 {args.docs_dir} 中未找到支持的文献文件")
    elif not args.demo:
        print(f"⚠️ 文献目录 {args.docs_dir} 不存在，使用 --demo 上传演示文献")

    # 4. 输出工作流配置提示
    print(f"""
{'='*60}
✅ 知识库配置完成！

请将以下知识库 ID 更新到工作流配置文件中：
  文件：workflow/immunology_rag_workflow.yml
  位置：knowledge-retrieval-node > dataset_configs > datasets > id
  替换：IMMUNOLOGY_DATASET_ID  →  {dataset_id}

{'='*60}
""")


if __name__ == "__main__":
    main()
