# immune-rag

<div align="center">

**AI-Powered PubMed Literature RAG System**

*A complete pipeline from PubMed batch download → RAG knowledge base → LLM-powered Q&A for immune medical research*

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

</div>

---

## 🔥 What It Does

immune-rag is an end-to-end AI system for immune medical research literature:

```
PubMed Search → Batch Download → Immune Keyword Filtering → RAG Knowledge Base → LLM Q&A
```

| Stage | Description |
|-------|-------------|
| **1. Batch Download** | Download 100K+ papers from PubMed with 5-tier fallback strategy |
| **2. Smart Filtering** | Filter papers by immune-related keywords (50+ indicators) |
| **3. Knowledge Base** | Build vector database with domain-specific embeddings |
| **4. LLM Q&A** | Chat with your immune literature corpus using DeepSeek/Qwen |

---

## ✨ Key Features

### 📥 Multi-Source Download Engine
- **5-tier fallback**: PMC XML → PMC PDF → Unpaywall → Sci-Hub → Publisher
- **Resume capability**: Skip already downloaded papers
- **Rate limiting**: Respect PubMed API limits with API key support
- **Year-based circuit breaker**: Skip invalid Sci-Hub requests

### 🧠 Immune-Specific Filtering
- Pre-built immune indicator list (50+ keywords)
- Flexible custom keyword support
- Journal blacklist/whitelist
- Duplicate detection across sources

### 💬 RAG-Powered Q&A
- Domain-specific embeddings (Qwen)
- SQLite-based metadata storage
- Skill configuration system for different query types
- Citation-grounded answers

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Create `.env` file:

```bash
# PubMed API (required)
EMAIL="your.email@example.com"
PUBMED_API_KEY="your_api_key"  # Optional, get from NCBI

# LLM API (for Q&A)
OPENAI_API_KEY="sk-..."  # Or other compatible APIs
```

### 3. Download Literature

```bash
# Download by immune keywords + journal list
python download_by_indicators.py

# Or download by journal list only
python pubmed_downloader_v4_enhanced.py
```

### 4. Build Knowledge Base

```bash
cd knowledge_base
python build_knowledge_base.py
```

### 5. Chat with Your Literature

```bash
cd knowledge_base
python chat_with_kb.py
```

---

## 📁 Project Structure

```
immune-rag/
├── pubmed_downloader_v4_enhanced.py  # Core download engine
├── filter_immune_papers.py          # Immune keyword filtering
├── download_by_indicators.py        # Combined download workflow
├── requirements.txt                 # Python dependencies
├── immune_keywords.txt               # Immune indicator list
├── journal_list.txt                  # Target journals
├── knowledge_base/
│   ├── build_knowledge_base.py       # KB construction
│   └── README.md                     # KB module docs
└── README.md
```

---

## 📊 Scale & Performance

| Metric | Value |
|--------|-------|
| Papers Downloaded | 100,000+ |
| Immune Indicators | 50+ |
| Journals Covered | 789 |
| Download Success Rate | 95%+ |
| Vector DB Size | ~2GB |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Download | Biopython, Requests, urllib3 |
| Filtering | Python, SQLite |
| Embeddings | Qwen Embedding, sentence-transformers |
| LLM | DeepSeek, Qwen (OpenAI-compatible API) |
| Knowledge Base | FAISS/Chroma, SQLite |
| Deployment | Docker-ready |

---

## 📖 Use Cases

| User | Benefit |
|------|---------|
| **Immunology Researchers** | Quickly build domain-specific literature corpus |
| **Pharma Companies** | Monitor competitive landscape in immunology |
| **Medical AI Developers** | Ready-to-use RAG pipeline for medical literature |
| **Graduate Students** | Efficiently gather research materials for thesis |

---

## ⚠️ Disclaimer

- This tool is for **research purposes only**
- Respect PubMed's Terms of Service
- Sci-Hub usage depends on your institution's policy
- Always cite sources properly in academic work

---

## 📬 Contact & Support

- **Author**: Luke Chen
- **GitHub**: [@lukechen-ai](https://github.com/lukechen-ai)
- **Issues**: Open an issue on GitHub for questions or feature requests

---

<p align="center">
  Built with ❤️ for the immunology research community
</p>
