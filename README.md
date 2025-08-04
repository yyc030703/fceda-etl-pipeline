# FCEDA Business Data Automation & Visualization  

**Automating and Accelerating Business Data Processing for Fairfax County Economic Development Authority**  
*George Mason University | MIS 462 Professional Readiness Experiential Program (PREP)*  

---

## 👥 Team Members  
- Rema Alharthi  
- Victor Luque  
- Hailey Ortega  
- Chau Phan  
- Ellie Yang  

**Faculty Advisor:** Dr. Brian K. Ngac  
**Partner Agency:** Fairfax County Economic Development Authority (FCEDA)  

---

## 📌 Project Overview  

Public business registration data in Virginia provides valuable insights for economic development but is often **messy, inconsistent, and time-consuming to process**.  
The FCEDA needed a **scalable, automated solution** to clean, validate, and visualize this data to enhance outreach efforts and strategic planning.  

Our team developed a **cloud-based ETL pipeline** leveraging AWS services and an **interactive Tableau dashboard** to transform raw datasets into actionable intelligence.  

---

## ❌ Problem Statement  

Prior to this project, FCEDA faced significant challenges:  
- Manual, time-intensive data cleaning slowed business outreach  
- Difficulty validating whether businesses were physically located within Fairfax County  
- Limited ability to visualize trends by city, ZIP code, or industry over time  

---

## ✅ Our Solution  

We designed a **serverless data processing and visualization platform** that:  

-  **Automates** ingestion, cleaning, and standardization of Virginia business registration data  
-  **Geocodes** addresses to determine Fairfax County boundaries  
-  **Visualizes** data with a user-friendly Tableau dashboard  

### 🔑 Key Components  
- **AWS Lambda + S3:** Serverless pipeline for cleaning, merging, and enriching datasets  
- **Amazon Location Service:** Adds latitude and longitude to enable spatial filtering  
- **Tableau Dashboard:** Interactive business intelligence tool with advanced filters and mapping  
- **Outputs:** Cleaned CSV datasets automatically prepared for Tableau, Salesforce, and ArcGIS  

---

## 🛠️ Tech Stack  

- **AWS Lambda (Python)**  
- **Amazon S3**  
- **Amazon Location Service**  
- **AWS Elastic Container Registry (ECR)** – for Docker-based geospatial processing  
- **Tableau** – real-time business data visualization  
- **Pandas, GeoPandas, Boto3** – data processing and AWS integrations  
 
---

## 🚀 Impact  

- Reduced manual processing from **hours to ~30 minutes**  
- Cleaned **1.85M raw records** down to **101K accurate, geocoded business records**  
- Delivered a reusable, scalable ETL solution with minimal monthly cost (< $100)  
- Empowered FCEDA teams with faster, data-driven outreach and planning capabilities  
