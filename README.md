# Srjana Distributed Service (Base)

This repository contains the baseline implementation of the **Srjana** distributed platform service (`com.zebra.ai.distributed`).

The service manages local peer-to-peer discovery using UDP multicast and facilitates secure, high-throughput file/data transfers between active network nodes utilizing gRPC.

---

## ⚠️ Crucial Build Requirement: Local SDK Libraries

To comply with repository size limits and protect proprietary intellectual property, **the heavy machine learning models and SDK binary dependencies are excluded from this Git repository**. 

Before building the project in Android Studio or compiling via Gradle, you must manually populate the local `libs` directory. **Failing to do so will result in compilation errors.**

### Required Libraries to Download:
1. **`emc-ai-suite-release-4.1.0.aar`** (~62.04 MB)  
   *Core Frontline AI Suite runtime API bindings.*
2. **`product-and-shelf-recognizer-3.4.5.aar`** (~360.73 MB)  
   *On-device deep learning model and inference engine for physical inventory and shelf classification.*
3. **`text-ocr-recognizer-2.10.1.aar`** (~402.65 MB)  
   *Edge-optimized character-recognition system used to ingest packaging labels and device serial values.*

---

## 🛠️ Setup Instructions

### Step 1: Obtain the Binaries
Download the matching versions of the above `.aar` files from the internal **Zebra Developer Portal / Zebra Frontline AI Enablers Portal** or retrieve them from your team's local development backup.

### Step 2: Place in Project Library Directory
Navigate to your project root and drop the three `.aar` files into the following directory structure:
```text
DistributedService/
└── app/
    └── libs/
        ├── emc-ai-suite-release-4.1.0.aar          <-- DROP HERE
        ├── product-and-shelf-recognizer-3.4.5.aar  <-- DROP HERE
        └── text-ocr-recognizer-2.10.1.aar          <-- DROP HERE
