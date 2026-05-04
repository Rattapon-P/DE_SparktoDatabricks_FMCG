# ข้อมูลดิบ — Instacart Market Basket Analysis

> English version: [README.md](README.md)

---

## โฟลเดอร์นี้ทำอะไร?

โฟลเดอร์ `raw/` คือที่เก็บ **ข้อมูลดิบต้นฉบับ** ที่ดาวน์โหลดมาจาก Kaggle ก่อนที่จะผ่านกระบวนการ ETL ใดๆ

ไฟล์ CSV จริงถูก **gitignore** (ไม่ push ขึ้น GitHub) เพราะไฟล์ใหญ่ที่สุดหนัก **577 MB** เกินขีดจำกัด 100 MB ของ GitHub  
แทนที่จะ push ไฟล์จริง เราเก็บ **schema** ไว้ใน README นี้เพื่อให้ทุกคนที่อ่าน repo เข้าใจโครงสร้างข้อมูลได้  
และมีโฟลเดอร์ [`sample/`](sample/) ที่บรรจุข้อมูลย่อ 100 orders (~46 KB) ไว้ทดสอบ pipeline โดยไม่ต้องโหลดไฟล์จริง

---

## ทำไมถึงเลือก dataset นี้?

โปรเจกต์นี้เตรียมสำหรับสัมภาษณ์ตำแหน่ง **Data Engineer ที่ Gosoft (CP All / กลุ่ม 7-Eleven)**  
Instacart เป็น dataset สาธารณะที่ใกล้เคียงกับข้อมูล retail / FMCG จริงมากที่สุด เพราะมี:

| ลักษณะ | ความหมาย |
|---|---|
| Product hierarchy | สินค้าแบ่งเป็น department → aisle → product เหมือน planogram ของร้านสะดวกซื้อ |
| Basket structure | แต่ละ order มีหลายสินค้า เหมือนใบเสร็จ POS จาก 7-Eleven |
| Reorder behaviour | รู้ว่าลูกค้าซื้อซ้ำหรือเปล่า ใช้วิเคราะห์ loyalty ได้ |
| ขนาดใหญ่จริง | 32 ล้าน rows ทดสอบ Spark performance ได้จริง ไม่ใช่ toy data |

---

## แหล่งข้อมูล

- **Dataset**: Instacart Market Basket Analysis
- **ผู้ให้ข้อมูล**: Instacart เปิดให้ใช้ในการแข่งขัน Kaggle ปี 2017
- **License**: เฉพาะงานวิจัย / ไม่ใช่เชิงพาณิชย์ ดูเงื่อนไขจาก Kaggle
- **ดาวน์โหลด**: <https://www.kaggle.com/competitions/instacart-market-basket-analysis/data>

หลังดาวน์โหลด ให้ unzip ไฟล์ทั้งหมดลง **โฟลเดอร์นี้** (`data/raw/`) โดยตรง

---

## รายการไฟล์ทั้งหมด

| ไฟล์ | จำนวน Rows | ขนาด | หน่วยข้อมูล (Grain) |
|---|---:|---:|---|
| `aisles.csv` | 134 | 2.5 KB | 1 row = 1 ทางเดิน (aisle) ในร้าน |
| `departments.csv` | 21 | 270 B | 1 row = 1 แผนกสินค้า |
| `products.csv` | 49,688 | 2.1 MB | 1 row = 1 สินค้า |
| `orders.csv` | 3,421,083 | 109 MB | 1 row = 1 คำสั่งซื้อ (header ของบิล) |
| `order_products__prior.csv` | 32,434,489 | **577 MB** | 1 row = 1 สินค้าใน 1 บิล (ออเดอร์เก่าทั้งหมด) |
| `order_products__train.csv` | 1,384,617 | 24 MB | 1 row = 1 สินค้าใน 1 บิล (ชุด train สำหรับ ML) |

> **หมายเหตุ:** `order_products__prior.csv` หนักมาก ถ้าจะ import เข้า Databricks ให้ใช้สคริปต์ [`split_prior.py`](split_prior.py) ก่อน (ดูวิธีใช้ด้านล่าง)

---

## อธิบายแต่ละไฟล์แบบละเอียด

### `aisles.csv` — ทางเดินในร้าน

ไฟล์เล็กที่สุด เก็บชื่อทางเดิน (เหมือน zone ในร้าน) เช่น `fresh vegetables`, `yogurt`, `energy granola bars`  
ใช้ join กับ `products.csv` เพื่อรู้ว่าสินค้าอยู่ zone ไหน

| Column | Type | ความหมาย |
|---|---|---|
| `aisle_id` | int | รหัสทางเดิน (Primary Key) |
| `aisle` | string | ชื่อทางเดิน เช่น `prepared soups salads` |

---

### `departments.csv` — แผนกสินค้า

ระดับบนสุดของ hierarchy สินค้า แผนกคือการจัดกลุ่มหลัก เช่น `frozen`, `produce`, `bakery`, `beverages`

| Column | Type | ความหมาย |
|---|---|---|
| `department_id` | int | รหัสแผนก (Primary Key) |
| `department` | string | ชื่อแผนก เช่น `frozen`, `produce`, `bakery` |

---

### `products.csv` — รายการสินค้า

ไดเรกทอรีสินค้าทั้งหมด ~50,000 รายการ แต่ละชิ้นบอกว่าอยู่ aisle และ department ไหน  
นี่คือ **dimension table** หลักของ data model — ทุกตารางที่มี `product_id` ต้อง join มาที่นี่

| Column | Type | ความหมาย |
|---|---|---|
| `product_id` | int | รหัสสินค้า (Primary Key) |
| `product_name` | string | ชื่อสินค้า เช่น `Organic Hass Avocado` |
| `aisle_id` | int | FK → `aisles.aisle_id` — บอกว่าอยู่ aisle ไหน |
| `department_id` | int | FK → `departments.department_id` — บอกว่าอยู่แผนกไหน |

---

### `orders.csv` — ตารางคำสั่งซื้อ (header ของบิล)

ไฟล์นี้เหมือน **ใบปะหน้าของบิล** — บอกว่าใครสั่ง เมื่อไหร่ เป็นออเดอร์ที่เท่าไหร่ของลูกค้าคนนั้น  
**ยังไม่มีรายการสินค้า** — รายการสินค้าอยู่ใน `order_products__*.csv` แยกต่างหาก

| Column | Type | ความหมาย |
|---|---|---|
| `order_id` | int | รหัสออเดอร์ (Primary Key) |
| `user_id` | int | รหัสลูกค้า |
| `eval_set` | string | `prior` = ออเดอร์เก่าทั้งหมด / `train` = ออเดอร์ล่าสุด (ชุดฝึก ML) / `test` = ชุดทดสอบ |
| `order_number` | int | ลำดับที่ของออเดอร์นี้ของลูกค้าคนนั้น (เริ่มจาก 1) |
| `order_dow` | int | วันในสัปดาห์ที่สั่ง (0 = อาทิตย์, 6 = เสาร์) |
| `order_hour_of_day` | int | ชั่วโมงที่สั่ง (0–23) |
| `days_since_prior_order` | float | กี่วันนับจากออเดอร์ก่อนหน้า (null ถ้าเป็นออเดอร์แรกของลูกค้า) |

---

### `order_products__prior.csv` — สินค้าในบิลเก่า (ไฟล์ใหญ่ที่สุด)

ไฟล์นี้คือ **fact table หลัก** ของ dataset — เก็บทุก line item ของทุกออเดอร์เก่า (prior)  
32 ล้าน rows = 32 ล้านครั้งที่ลูกค้าหยิบสินค้าใส่ตะกร้า ใช้วิเคราะห์พฤติกรรมการซื้อซ้ำและ basket composition

| Column | Type | ความหมาย |
|---|---|---|
| `order_id` | int | FK → `orders.order_id` — บิลนี้คือบิลไหน |
| `product_id` | int | FK → `products.product_id` — สินค้าชิ้นไหน |
| `add_to_cart_order` | int | หยิบเป็นชิ้นที่เท่าไหร่ในตะกร้า (เริ่มจาก 1) |
| `reordered` | int | **1** = เคยซื้อสินค้านี้มาก่อน, **0** = ซื้อครั้งแรก |

> **ขนาด 577 MB** — ดูวิธีตัด/แบ่งไฟล์นี้ก่อน import Databricks ในหัวข้อถัดไป

---

### `order_products__train.csv` — สินค้าในบิล train set

ไฟล์นี้คือ **ออเดอร์ล่าสุด** ของลูกค้าแต่ละคน (1 คน = 1 ออเดอร์ใน train set)  
ใช้เป็น label สำหรับ ML — ทาย reorder จากประวัติใน prior แล้ว validate กับ train  
สำหรับงาน Data Engineering ไฟล์นี้ใช้สร้าง Silver layer เพื่อ union หรือ join กับ prior

| Column | Type | ความหมาย |
|---|---|---|
| `order_id` | int | FK → `orders.order_id` |
| `product_id` | int | FK → `products.product_id` |
| `add_to_cart_order` | int | ลำดับที่หยิบในตะกร้า |
| `reordered` | int | 1 = เคยซื้อมาก่อน, 0 = ครั้งแรก |

---

## ความสัมพันธ์ระหว่างไฟล์ (Entity Relationship)

```
departments (21 แผนก)
    └── department_id
            ↑
aisles (134 ทางเดิน)      products (49,688 สินค้า)
    └── aisle_id  ───────→  aisle_id
                             department_id
                             product_id
                                  ↑
order_products__prior  ─────→ product_id
order_products__train  ─────→ product_id
    └── order_id ───────────────────────→ orders (3.4M ออเดอร์)
                                                └── user_id (ลูกค้า)
```

ใน pipeline ของโปรเจกต์นี้ข้อมูลไหลผ่าน 3 ชั้น:

| Layer | ทำอะไร |
|---|---|
| **Bronze** | อ่าน CSV ดิบ เก็บเป็น Delta Table ไม่แก้ไขข้อมูลเลย |
| **Silver** | join orders + order_products + products, ทำความสะอาด, cast type ให้ถูกต้อง |
| **Gold** | aggregate เช่น ยอดขายต่อ department, top reorder products, peak shopping hour |

---

## ตัด / แบ่งไฟล์ใหญ่ก่อน Import Databricks

ไฟล์ `order_products__prior.csv` หนัก **577 MB** — Databricks Community Edition มี storage จำกัด  
ใช้สคริปต์ [`split_prior.py`](split_prior.py) ที่เตรียมไว้:

```bash
# โหมด 1: ตัดเหลือ 30% → order_products__prior_30pct.csv (~173 MB)
python data/raw/split_prior.py

# โหมด 2: ตัดเหลือ % ที่กำหนดเอง เช่น 50%
python data/raw/split_prior.py --pct 50

# โหมด 3: แบ่งเป็น 2 ไฟล์เท่าๆ กัน → _1.csv / _2.csv (~289 MB ต่อไฟล์)
python data/raw/split_prior.py --split

# โหมด 4: แบ่งเป็น 2 ไฟล์ ไฟล์ละ 40%
python data/raw/split_prior.py --split --pct 40
```

---

## ข้อมูล Sample (ทดสอบโดยไม่ต้องโหลดไฟล์จริง)

โฟลเดอร์ [`sample/`](sample/) สร้างโดย [`generate_samples.py`](generate_samples.py)  
ตัด 100 orders แรกออกมา แล้ว filter ตารางที่เกี่ยวข้องให้ครบ — foreign key ยังคงถูกต้องทุกตาราง

| ไฟล์ | จำนวน rows ตัวอย่าง |
|---|---:|
| `sample/orders.csv` | 100 |
| `sample/order_products__train.csv` | 104 |
| `sample/order_products__prior.csv` | 1,011 |
| `sample/products.csv` | 505 |
| `sample/aisles.csv` | 91 |
| `sample/departments.csv` | 20 |

รัน generate ใหม่หลังจากวางไฟล์จริงแล้ว:

```bash
python data/raw/generate_samples.py
```
