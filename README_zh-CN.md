# immune-rag

<div align="center">

**AI 驱动的 PubMed 文献 RAG 系统**

*从 PubMed 批量下载 → RAG 知识库 → LLM 智能问答，一站式解决免疫医学研究文献分析*

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

</div>

---

## 🔥 项目介绍

immune-rag 是一个面向免疫医学研究的端到端 AI 系统：

```
PubMed 检索 → 批量下载 → 免疫关键词过滤 → RAG 知识库 → LLM 智能问答
```

| 阶段 | 说明 |
|------|------|
| **1. 批量下载** | 5 层降级策略下载 10 万+ 篇文献 |
| **2. 智能过滤** | 按免疫相关关键词过滤（50+ 指标） |
| **3. 知识库** | 使用领域特定 Embedding 构建向量数据库 |
| **4. LLM 问答** | 使用 DeepSeek/Qwen 与文献对话 |

---

## ✨ 核心功能

### 📥 多源下载引擎
- **5 层降级策略**：PMC XML → PMC PDF → Unpaywall → Sci-Hub → 出版社官网
- **断点续传**：跳过已下载文献
- **访问限流**：尊重 PubMed API 限制，支持 API Key
- **年份熔断**：跳过无效的 Sci-Hub 请求

### 🧠 免疫特异性过滤
- 预置免疫指标列表（50+ 关键词）
- 支持自定义关键词
- 期刊黑名单/白名单
- 多源去重

### 💬 RAG 智能问答
- 领域特定 Embedding（Qwen）
- SQLite 元数据存储
- 技能配置系统适配不同查询类型
- 带引用的答案

---

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境

创建 `.env` 文件：

```bash
# PubMed API（必填）
EMAIL="your.email@example.com"
PUBMED_API_KEY="your_api_key"  # 可选，从 NCBI 获取

# LLM API（用于问答）
OPENAI_API_KEY="sk-..."  # 或其他兼容 API
```

### 3. 下载文献

```bash
# 按免疫关键词 + 期刊列表下载
python download_by_indicators.py

# 或仅按期刊列表下载
python pubmed_downloader_v4_enhanced.py
```

### 4. 构建知识库

```bash
cd knowledge_base
python build_knowledge_base.py
```

### 5. 与文献对话

```bash
cd knowledge_base
python chat_with_kb.py
```

---

## 📁 项目结构

```
immune-rag/
├── pubmed_downloader_v4_enhanced.py  # 核心下载引擎
├── filter_immune_papers.py          # 免疫关键词过滤
├── download_by_indicators.py        # 组合工作流
├── requirements.txt                 # Python 依赖
├── immune_keywords.txt               # 免疫指标列表
├── journal_list.txt                  # 目标期刊列表
├── knowledge_base/
│   ├── build_knowledge_base.py       # 知识库构建
│   └── README.md                     # 知识库模块文档
└── README.md
```

---

## 📊 规模与性能

| 指标 | 数值 |
|------|------|
| 下载文献量 | 100,000+ |
| 免疫指标数 | 50+ |
| 覆盖期刊 | 789 |
| 下载成功率 | 95%+ |
| 向量库大小 | ~2GB |

---

## 🛠 技术栈

| 层级 | 技术 |
|------|------|
| 下载 | Biopython, Requests, urllib3 |
| 过滤 | Python, SQLite |
| Embedding | Qwen Embedding, sentence-transformers |
| LLM | DeepSeek, Qwen (OpenAI 兼容 API) |
| 知识库 | FAISS/Chroma, SQLite |
| 部署 | Docker 可部署 |

---

## 📖 使用场景

| 用户 | 收益 |
|------|------|
| **免疫学研究人员** | 快速构建领域专用文献库 |
| **制药公司** | 监测免疫领域竞争态势 |
| **医疗 AI 开发者** | 开箱即用的医学文献 RAG 流程 |
| **研究生** | 高效收集论文素材 |

---

## ⚠️ 免责声明

- 本工具仅用于**研究目的**
- 请遵守 PubMed 服务条款
- Sci-Hub 使用取决于你所在机构的政策
- 学术工作中请务必正确引用来源

---

## 📬 联系与支持

- **作者**：Luke Chen
- **GitHub**：[@lukechen-ai](https://github.com/lukechen-ai)
- **问题反馈**：欢迎在 GitHub 提交 Issue

---

<p align="center">
  为免疫学研究社区用心打造 ❤️
</p>

---

*[English version](README.md)*
