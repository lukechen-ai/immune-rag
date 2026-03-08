# IT-X 免疫学文献 RAG 问答系统

基于 [Dify](https://dify.ai) 平台的免疫学文献检索增强生成（RAG）工作流，支持从免疫学文献知识库中精准检索相关内容，结合大语言模型提供专业、循证的医学解答。

---

## 系统架构

```
用户问题
   │
   ▼
┌─────────────────┐
│   查询改写节点   │  ← 将自然语言问题转化为专业检索查询
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  知识库检索节点  │  ← 混合检索（语义 + 关键词）+ Rerank
│  (Hybrid RAG)   │  ← Top-K=8，相关度阈值=0.3
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
  有结果    无结果
    │         │
    ▼         ▼
┌───────┐  ┌────────┐
│专业   │  │ 兜底   │
│医学   │  │ 回答   │
│解答   │  │ 节点   │
└───┬───┘  └───┬────┘
    └─────┬────┘
          ▼
       输出结果
```

## 工作流节点说明

| 节点 | 类型 | 说明 |
|------|------|------|
| **开始节点** | Start | 接收用户免疫学问题（最长2000字符） |
| **查询改写** | LLM | 将问题转化为包含专业医学术语的检索查询 |
| **知识库检索** | Knowledge Retrieval | 混合检索，返回Top-8相关文献段落，支持Rerank |
| **条件判断** | If-Else | 判断是否检索到相关文献 |
| **专业医学解答** | LLM | 基于文献生成结构化专业解答（含文献引用） |
| **兜底回答** | LLM | 无相关文献时提供基础解答并引导获取专业资源 |
| **结束节点** | End | 输出最终回答 |

---

## 快速开始

### 前置要求

- Dify 实例（自托管或云端）v0.6.0+
- 配置好 LLM Provider（推荐：Claude Sonnet / GPT-4o）
- 配置好 Embedding Provider（推荐：OpenAI text-embedding-3-large）
- 配置好 Rerank Provider（推荐：Cohere rerank-multilingual-v3.0）

### 步骤一：导入工作流

1. 登录 Dify 控制台
2. 点击「工作室」→「创建应用」→「从 DSL 导入」
3. 上传 `workflow/immunology_rag_workflow.yml`
4. 确认导入成功

### 步骤二：构建知识库

#### 方式A：使用脚本自动构建（推荐）

```bash
# 安装依赖
pip install requests tqdm

# 上传内置演示文献（4篇高质量免疫学文献摘要）
python scripts/setup_knowledge_base.py \
    --api-base https://your-dify-instance.com \
    --api-key your-dataset-api-key \
    --demo

# 上传本地文献文件（支持 PDF/TXT/MD/DOCX）
python scripts/setup_knowledge_base.py \
    --api-base https://your-dify-instance.com \
    --api-key your-dataset-api-key \
    --docs-dir ./knowledge_base/papers
```

> 获取 Dataset API Key：Dify → 知识库 → API → 创建密钥

#### 方式B：手动上传

1. Dify 控制台 → 「知识库」→ 「创建知识库」
2. 选择文献文件上传（支持 PDF、TXT、Markdown、Word）
3. 配置分块参数：
   - 分隔符：`\n\n`
   - 最大分块：**1000 tokens**
   - 重叠：**150 tokens**
4. 选择向量化模型和 Rerank 模型

### 步骤三：绑定知识库到工作流

1. 打开导入的工作流
2. 点击「知识库检索」节点
3. 在「知识库」配置中选择刚创建的免疫学文献知识库
4. 或手动编辑 `workflow/immunology_rag_workflow.yml` 中的 `IMMUNOLOGY_DATASET_ID`

### 步骤四：配置模型

在工作流设置中确认以下模型配置：

| 节点 | 推荐模型 | 说明 |
|------|---------|------|
| 查询改写 | claude-sonnet-4-6 | 专业术语转换，低 temperature |
| 专业医学解答 | claude-sonnet-4-6 | 长文本生成，temperature=0.3 |
| 兜底回答 | claude-haiku-4-5 | 简洁快速回复 |

---

## 知识库建设建议

### 推荐文献来源

| 来源 | 类型 | 获取方式 |
|------|------|---------|
| [PubMed](https://pubmed.ncbi.nlm.nih.gov/) | 原始论文 | 开放获取 PDF |
| [Nature Immunology](https://www.nature.com/ni/) | 高影响力综述 | 机构订阅 |
| [Immunity](https://www.cell.com/immunity/) | 研究论文 | 机构订阅 |
| [Journal of Experimental Medicine](https://www.jem.org/) | 经典研究 | 开放获取 |
| [UpToDate](https://www.uptodate.com/) | 临床指南 | 订阅导出 |
| 《基础免疫学》教材 | 基础知识 | 章节文本 |

### 推荐覆盖方向

- [ ] 先天免疫与适应性免疫基础机制
- [ ] T细胞/B细胞发育与功能
- [ ] 细胞因子网络与信号通路
- [ ] 自身免疫性疾病（SLE、RA、MS、T1D）
- [ ] 肿瘤免疫学与免疫检查点抑制剂
- [ ] CAR-T细胞疗法
- [ ] 疫苗学与免疫记忆
- [ ] 移植免疫与排斥反应
- [ ] 感染性免疫（细菌、病毒、寄生虫）
- [ ] 过敏与超敏反应
- [ ] 免疫缺陷病

---

## 回答示例

**用户问题**：PD-1/PD-L1免疫检查点抑制剂治疗后为什么会出现免疫相关不良事件？

**系统回答**：

### 📋 问题概述
PD-1/PD-L1抑制剂引起的免疫相关不良事件（irAE）是打破免疫自稳后累及正常组织的全身性免疫激活现象。

### 🔬 机制解析
1. **免疫耐受破坏**：PD-1/PD-L1通路在正常组织中维持外周免疫耐受，抑制后自身反应性T细胞被释放...
2. **交叉反应**：肿瘤抗原与正常组织抗原之间的分子模拟导致器官特异性攻击...

### 📚 文献依据
[PD-1/PD-L1免疫检查点抑制剂综述] 指出：PD-1通过SHP-2磷酸酶抑制TCR下游信号，阻断该通路后自身反应性T细胞恢复活性...

---

## 项目结构

```
IT-X-immune-AI/
├── workflow/
│   └── immunology_rag_workflow.yml   # Dify 工作流 DSL（可直接导入）
├── scripts/
│   └── setup_knowledge_base.py       # 知识库构建脚本
├── knowledge_base/
│   └── papers/                       # 放置本地文献文件（PDF/TXT等）
└── README.md
```

---

## 注意事项

> **医疗免责声明**：本系统基于文献资料提供免疫学知识参考，仅供专业人员学习和研究使用，
> **不构成临床诊疗建议**。所有临床决策应在执业医师指导下进行。

- 系统回答质量取决于知识库文献的质量和覆盖度
- 建议定期更新知识库，纳入最新免疫学研究进展
- 对于罕见病或前沿治疗，建议结合最新文献和专家咨询
