# Market-data-pipeline
A secure market data pipeline that fetches data from yfinance, obfuscates asset identifiers, and resolves them via a registry-based mapping system.
Secure Market Data Obfuscation Registry
📌 Overview

This project demonstrates a secure, registry-driven data pipeline architecture for financial market data.

It fetches market data using the Python library yfinance, obfuscates asset identifiers (e.g., tickers), and stores the mapping inside a registry. A separate resolution pipeline uses the registry to map obfuscated asset IDs back to their original identifiers.

The goal is to simulate how production systems abstract and protect internal identifiers while maintaining traceability.

❓ Problem Statement

How can we securely obfuscate financial asset identifiers while still enabling reliable resolution back to the original assets?

In financial systems, exposing raw asset identifiers (such as tickers) can introduce:

Security risks

Tight coupling between systems

Lack of abstraction

Compliance concerns

This project solves the problem using a registry-based mapping architecture that separates public-facing identifiers from internal asset identities.

🏗 Solution Architecture

The system consists of two decoupled pipelines:

1️⃣ Data Ingestion & Obfuscation Pipeline

Fetches market data from yfinance

Generates an obfuscated asset ID

Stores mapping (real ID ↔ obfuscated ID) in a registry

Persists obfuscated market data

Flow:

Market Data Source
↓
yfinance Fetch Layer
↓
Obfuscation Engine
↓
Asset Registry
↓
Obfuscated Data Store

2️⃣ Registry Resolution Pipeline

Accepts obfuscated asset ID

Looks up original asset identifier in registry

Resolves and returns actual asset details

Flow:

Obfuscated ID
↓
Registry Lookup
↓
Original Asset ID

🧠 Key Design Principles

Separation of concerns

Decoupled pipeline architecture

Registry-based mapping pattern

Identifier abstraction

Secure ID tokenization

Reversible mapping through controlled lookup

🛠 Tech Stack

Python

yfinance

Hashing / Tokenization Logic

File-based or in-memory registry storage

📂 Project Structure (Example)
├── ingest_pipeline.py
├── resolve_pipeline.py
├── registry.json
├── obfuscated_data.json
└── README.md
▶️ How to Run
Step 1: Install Dependencies
pip install yfinance
Step 2: Run Ingestion Pipeline
python ingest_pipeline.py

This will:

Fetch market data

Generate obfuscated asset IDs

Store mappings in registry

Save obfuscated data

Step 3: Run Resolution Pipeline
python resolve_pipeline.py

This will:

Accept obfuscated ID

Resolve it using registry

Return original asset identifier

🔐 Why Obfuscation?

In real-world financial and distributed systems:

Internal identifiers are often hidden

External systems operate on abstracted IDs

Registry systems manage controlled mapping

Security and decoupling are critical

This project simulates such production-grade architectural thinking.

🚀 Use Cases

Secure financial data pipelines

Tokenized identifier systems

API abstraction layers

Multi-system ID mapping

Registry-driven microservices

🎯 What This Project Demonstrates

Data engineering fundamentals

Secure identifier handling

Architecture-level thinking

Clean pipeline separation

Real-world system design patterns

📌 Future Improvements

Database-backed registry (PostgreSQL / Redis)

REST API interface

Dockerization

Logging & monitoring

Encryption-based token generation

CI/CD integration

📜 License

This project is for educational and architectural demonstration purposes.
