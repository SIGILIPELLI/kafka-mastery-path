---
title: "Free Apache Kafka Course: Topics, Partitions, Producers, Consumers, Delivery Semantics, Connect, Streams & KRaft"
description: "Free Kafka course: 40 lessons on topics/partitions, producers/consumers, delivery semantics, replication, Connect, Streams and KRaft."
---

# Kafka Mastery Path

Most people meet Kafka through a confusing analogy — "it's like a queue, but not really" — and never get the one idea that
actually explains everything else: a Kafka topic is a **durable, append-only log**, split into partitions, and every
consumer just remembers its own position in that log. Once that's real to you, a lot of things stop being magic: why two
consumers in the same group never read the same partition, why "exactly once" needs a producer ID and a transaction
coordinator and isn't free, why a compacted topic can still have old keys if you haven't read past a tombstone's retention
window, and why a consumer that stops committing offsets isn't broken, it's just rewinding every time it restarts.

This course was written and run against a **real, local Apache Kafka 4.4.0 broker in KRaft mode** (no ZooKeeper — Kafka
removed it entirely as of version 4.0) on an ordinary laptop with Java 17+. No cloud account, no managed Kafka service, and
no Docker Compose cluster were needed for almost everything here: real topics, real producers and consumers, a real
cgroup-free single-broker cluster, real Kafka Connect (including a genuine classpath gotcha this course hit and explains),
and a real Kafka Streams application doing real stateful aggregation.

It assumes basic programming and command-line comfort. No prior messaging-system or distributed-systems experience is
assumed, though if you want Java itself in depth first, see the [Java Mastery Path](https://learnmasterypath.com/java-mastery-path/).

## How the program is organized

| Level | Focus | Modules | What you can do after |
|-------|-------|---------|------------------------|
| **1 · The Log, Topics & Partitions** | What Kafka is, brokers and KRaft, topics/partitions, producers, consumer groups, replication, delivery semantics, retention | 10 | Run a real cluster and reason correctly about ordering and delivery |
| **2 · Operating a Cluster** | Producer/consumer tuning, exactly-once, monitoring, log internals, security, reassignment, quotas | 10 | Configure and diagnose a cluster, not just use one |
| **3 · Kafka Connect & Kafka Streams** | Connect architecture and the REST API, Streams topology, state stores, joins/windows, schema evolution | 10 | Build real ingestion pipelines and stream-processing apps |
| **4 · Production Kafka & Capstone** | Performance, capacity planning, failure modes, disaster recovery, event-driven patterns, Kubernetes | 10 | Plan, operate, and troubleshoot Kafka in production |

Each level has 9 teaching modules plus a hands-on project (module 10), for **40 lessons** total.

## How to use this site

- **Run it yourself.** Download Kafka from [kafka.apache.org](https://kafka.apache.org/downloads), format storage with
  `kafka-storage.sh`, and start a broker — no ZooKeeper, no account, no cloud bill. Java 17 or newer is all you need.
- **Read the "what was run" note** at the top of each lesson. The overwhelming majority of this course's command output
  is real, observed output from a real broker; anything that genuinely couldn't be run locally (multi-datacenter
  replication at scale, a managed-service console) says so plainly.
- **Do the "predict, then run" exercises.** Kafka has several behaviors (rebalancing, compaction timing, exactly-once
  overhead) that are easy to mis-predict from the docs alone and genuinely clarifying to watch happen.
- **Do the projects.** Each one is a real, runnable pipeline or application, not a diagram.

## More from the Mastery Path series

Free, structured, module-wise training across 101 other languages, platforms and disciplines — expand a category to browse:

<div class="mastery-tracks">
<details class="mastery-card">
<summary>Languages <span class="mastery-count">20</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/python-mastery-path/">🐍 Python</a>
  <a href="https://learnmasterypath.com/java-mastery-path/">☕ Java</a>
  <a href="https://learnmasterypath.com/javascript-mastery-path/">🟨 JavaScript</a>
  <a href="https://learnmasterypath.com/typescript-mastery-path/">🔷 TypeScript</a>
  <a href="https://learnmasterypath.com/csharp-mastery-path/">🔵 C#</a>
  <a href="https://learnmasterypath.com/shell-mastery-path/">🐚 Shell/Bash</a>
  <a href="https://learnmasterypath.com/powershell-mastery-path/">💻 PowerShell</a>
  <a href="https://learnmasterypath.com/c-mastery-path/">🇨 C</a>
  <a href="https://learnmasterypath.com/cpp-mastery-path/">➕ C++</a>
  <a href="https://learnmasterypath.com/go-mastery-path/">🐹 Go</a>
  <a href="https://learnmasterypath.com/rust-mastery-path/">🦀 Rust</a>
  <a href="https://learnmasterypath.com/sql-mastery-path/">🗄️ SQL</a>
  <a href="https://learnmasterypath.com/ruby-mastery-path/">💎 Ruby</a>
  <a href="https://learnmasterypath.com/php-mastery-path/">🐘 PHP</a>
  <a href="https://learnmasterypath.com/kotlin-mastery-path/">🟣 Kotlin</a>
  <a href="https://learnmasterypath.com/swift-mastery-path/">🐦 Swift</a>
  <a href="https://learnmasterypath.com/dart-mastery-path/">🎯 Dart</a>
  <a href="https://learnmasterypath.com/scala-mastery-path/">🔴 Scala</a>
  <a href="https://learnmasterypath.com/r-mastery-path/">📊 R</a>
  <a href="https://learnmasterypath.com/matlab-mastery-path/">🟧 MATLAB</a>
</div>
</details>
<details class="mastery-card">
<summary>Web Frameworks <span class="mastery-count">11</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/react-mastery-path/">⚛️ React</a>
  <a href="https://learnmasterypath.com/nextjs-mastery-path/">▲ Next.js</a>
  <a href="https://learnmasterypath.com/angular-mastery-path/">🅰️ Angular</a>
  <a href="https://learnmasterypath.com/vue-mastery-path/">💚 Vue.js</a>
  <a href="https://learnmasterypath.com/html-css-mastery-path/">🎨 HTML & CSS</a>
  <a href="https://learnmasterypath.com/tailwind-mastery-path/">🌬️ Tailwind CSS</a>
  <a href="https://learnmasterypath.com/django-mastery-path/">🎸 Django</a>
  <a href="https://learnmasterypath.com/fastapi-mastery-path/">🚀 FastAPI</a>
  <a href="https://learnmasterypath.com/graphql-mastery-path/">🕸️ GraphQL</a>
  <a href="https://learnmasterypath.com/nodejs-mastery-path/">🟩 Node.js</a>
  <a href="https://learnmasterypath.com/spring-boot-mastery-path/">🍃 Spring Boot</a>
</div>
</details>
<details class="mastery-card">
<summary>Mobile <span class="mastery-count">2</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/flutter-mastery-path/">💙 Flutter</a>
  <a href="https://learnmasterypath.com/react-native-mastery-path/">📲 React Native</a>
</div>
</details>
<details class="mastery-card">
<summary>Testing & QA <span class="mastery-count">6</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/java-testing-mastery-path/">🧪 Java Testing</a>
  <a href="https://learnmasterypath.com/cpp-testing-mastery-path/">🧪 C/C++ Testing</a>
  <a href="https://learnmasterypath.com/python-testing-mastery-path/">🧪 Python Testing</a>
  <a href="https://learnmasterypath.com/automotive-testing-mastery-path/">🚗 Automotive Testing</a>
  <a href="https://learnmasterypath.com/selenium-mastery-path/">🧭 Selenium</a>
  <a href="https://learnmasterypath.com/cypress-mastery-path/">🌲 Cypress</a>
</div>
</details>
<details class="mastery-card">
<summary>Security <span class="mastery-count">2</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/cybersecurity-mastery-path/">🛡️ Cybersecurity</a>
  <a href="https://learnmasterypath.com/ethical-hacking-mastery-path/">🎯 Ethical Hacking</a>
</div>
</details>
<details class="mastery-card">
<summary>Cloud Platforms <span class="mastery-count">5</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/aws-mastery-path/">☁️ AWS</a>
  <a href="https://learnmasterypath.com/azure-mastery-path/">☁️ Azure</a>
  <a href="https://learnmasterypath.com/gcp-mastery-path/">☁️ GCP</a>
  <a href="https://learnmasterypath.com/ibm-cloud-mastery-path/">☁️ IBM Cloud</a>
  <a href="https://learnmasterypath.com/adobe-mastery-path/">🎨 Adobe</a>
</div>
</details>
<details class="mastery-card">
<summary>Data & Analytics <span class="mastery-count">7</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/data-engineering-mastery-path/">🛠️ Data Engineering</a>
  <a href="https://learnmasterypath.com/data-science-mastery-path/">📈 Data Science</a>
  <a href="https://learnmasterypath.com/tableau-mastery-path/">📊 Tableau</a>
  <a href="https://learnmasterypath.com/excel-mastery-path/">📗 Excel</a>
  <a href="https://learnmasterypath.com/pyspark-mastery-path/">⚡ PySpark</a>
  <a href="https://learnmasterypath.com/etl-datalake-mastery-path/">🏞️ ETL & Data Lake</a>
  <a href="https://learnmasterypath.com/power-bi-mastery-path/">📉 Power BI</a>
</div>
</details>
<details class="mastery-card">
<summary>Databases <span class="mastery-count">6</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/postgresql-mastery-path/">🐘 PostgreSQL</a>
  <a href="https://learnmasterypath.com/mongodb-mastery-path/">🍃 MongoDB</a>
  <a href="https://learnmasterypath.com/redis-mastery-path/">🟥 Redis</a>
  <a href="https://learnmasterypath.com/snowflake-mastery-path/">❄️ Snowflake</a>
  <a href="https://learnmasterypath.com/databricks-mastery-path/">🧱 Databricks</a>
  <a href="https://learnmasterypath.com/python-mastery-path/vector-databases-mastery-path/">🧭 Vector Databases</a>
</div>
</details>
<details class="mastery-card">
<summary>AI / ML / LLM <span class="mastery-count">12</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/ai-ml-mastery-path/">🤖 AI/ML</a>
  <a href="https://learnmasterypath.com/llm-dev-mastery-path/">🧠 LLM Dev</a>
  <a href="https://learnmasterypath.com/rag-mastery-path/">📚 RAG</a>
  <a href="https://learnmasterypath.com/edge-ai-mastery-path/">📱 Edge AI</a>
  <a href="https://learnmasterypath.com/claude-training-mastery-path/">🔶 Claude Training</a>
  <a href="https://learnmasterypath.com/ai-tools-mastery-path/">🧰 AI Tools</a>
  <a href="https://learnmasterypath.com/ml-math-mastery-path/">➗ ML Math Foundations</a>
  <a href="https://learnmasterypath.com/prompt-engineering-mastery-path/">✍️ Prompt Engineering</a>
  <a href="https://learnmasterypath.com/ai-agents-mastery-path/">🕹️ AI Agents</a>
  <a href="https://learnmasterypath.com/mlops-mastery-path/">🔁 MLOps</a>
  <a href="https://learnmasterypath.com/computer-vision-mastery-path/">👁️ Computer Vision</a>
  <a href="https://learnmasterypath.com/python-mastery-path/mcp-mastery-path/">🔌 MCP</a>
</div>
</details>
<details class="mastery-card">
<summary>Embedded Systems <span class="mastery-count">5</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/embedded-mastery-path/">🔌 Embedded</a>
  <a href="https://learnmasterypath.com/embedded-linux-mastery-path/">🐧 Embedded Linux</a>
  <a href="https://learnmasterypath.com/embedded-python-mastery-path/">🐍 Embedded Python</a>
  <a href="https://learnmasterypath.com/freertos-mastery-path/">⏱️ FreeRTOS</a>
  <a href="https://learnmasterypath.com/s32k-mastery-path/">🔧 S32K</a>
</div>
</details>
<details class="mastery-card">
<summary>Leadership & Management <span class="mastery-count">5</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/product-manager-mastery-path/">📋 Product Manager</a>
  <a href="https://learnmasterypath.com/product-lead-mastery-path/">🧭 Product Lead</a>
  <a href="https://learnmasterypath.com/project-manager-mastery-path/">📅 Project Manager</a>
  <a href="https://learnmasterypath.com/ai-manager-mastery-path/">🤖 AI Manager</a>
  <a href="https://learnmasterypath.com/servant-leadership-mastery-path/">🤝 Servant Leadership</a>
</div>
</details>
<details class="mastery-card">
<summary>Professional Skills <span class="mastery-count">4</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/english-fluency-mastery-path/">🗣️ English Fluency & IELTS</a>
  <a href="https://learnmasterypath.com/workday-mastery-path/">🧑‍💼 Workday</a>
  <a href="https://learnmasterypath.com/digital-marketing-mastery-path/">📣 Digital Marketing & SEO</a>
  <a href="https://learnmasterypath.com/tally-gst-mastery-path/">🧾 Tally Prime & GST</a>
</div>
</details>
<details class="mastery-card">
<summary>Careers & Interviews <span class="mastery-count">2</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/dsa-interview-mastery-path/">🧩 DSA & Coding Interviews</a>
  <a href="https://learnmasterypath.com/system-design-mastery-path/">🏗️ System Design</a>
</div>
</details>
<details class="mastery-card">
<summary>Process & APIs <span class="mastery-count">3</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/agile-mastery-path/">🔄 Agile/Scrum/Kanban</a>
  <a href="https://learnmasterypath.com/rest-api-mastery-path/">🔗 REST API</a>
  <a href="https://learnmasterypath.com/playwright-mastery-path/">🎭 Playwright</a>
</div>
</details>
<details class="mastery-card">
<summary>Infrastructure & Ops <span class="mastery-count">11</span></summary>
<div class="mastery-grid">
  <a href="https://learnmasterypath.com/server-ops-mastery-path/">🖥️ Server Ops</a>
  <a href="https://learnmasterypath.com/linux-mastery-path/">🐧 Linux</a>
  <a href="https://learnmasterypath.com/nodemcu-mastery-path/">📶 NodeMCU/IoT</a>
  <a href="https://learnmasterypath.com/terraform-mastery-path/">🌍 Terraform</a>
  <a href="https://learnmasterypath.com/kubernetes-mastery-path/">☸️ Kubernetes</a>
  <a href="https://learnmasterypath.com/docker-mastery-path/">🐳 Docker</a>
  <a href="https://learnmasterypath.com/github-mastery-path/">🐙 GitHub & Git</a>
  <a href="https://learnmasterypath.com/cicd-mastery-path/">🔁 CI/CD</a>
  <a href="https://learnmasterypath.com/ansible-mastery-path/">🤖 Ansible</a>
  <a href="https://learnmasterypath.com/microservices-mastery-path/">🧩 Microservices</a>
  <a href="https://learnmasterypath.com/observability-mastery-path/">📈 Observability</a>
</div>
</details>
</div>
