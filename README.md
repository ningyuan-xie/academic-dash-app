# Academic Data Analytics Platform

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![Dash](https://img.shields.io/badge/Dash-3.0.4-119DFF?logo=plotly&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-6.0.1-3F4F75?logo=plotly&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.0.3-000000?logo=flask&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-9.2.0-4479A1?logo=mysql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4.10.1-47A248?logo=mongodb&logoColor=white)
![Neo4j](https://img.shields.io/badge/Neo4j-5.28.1-008CC1?logo=neo4j&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.2.3-150458?logo=pandas&logoColor=white)

**CS411 Database Systems - Final Project**  
**Author:** Ningyuan Xie  
**Instructor:** Prof. Kevin Chen-Chuan Chang  
**Institution:** University of Illinois Urbana-Champaign

---

## Table of Contents
- [1. Title: GradXplorer](#1-title-gradxplorer)
- [2. Purpose](#2-purpose)
  - [2.1 Application Scenario](#21-application-scenario)
  - [2.2 Target Users](#22-target-users)
  - [2.3 Objectives](#23-objectives)
- [3. Demo](#3-demo)
- [4. Installation](#4-installation)
  - [4.1 Option 1: Install Packages from requirements.txt](#41-option-1-install-packages-from-requirementstxt)
  - [4.2 Option 2: Use Conda Environment from environment.yml](#42-option-2-use-conda-environment-from-environmentyml)
  - [4.3 Run the Application](#43-run-the-application)
- [5. Usage](#5-usage)
  - [5.1 Top 10 Trending Keywords in Publications](#51-top-10-trending-keywords-in-publications)
  - [5.2 User-selected Favorite Keywords](#52-user-selected-favorite-keywords)
  - [5.3 Faculty Members Relevant to Selected Keywords](#53-faculty-members-relevant-to-selected-keywords)
  - [5.4 Faculty Members with Highest KRC](#54-faculty-members-with-highest-krc)
  - [5.5 Top Keywords in Faculty Interests](#55-top-keywords-in-faculty-interests)
  - [5.6 Top Universities Collaborated With](#56-top-universities-collaborated-with)
- [6. Design](#6-design)
  - [6.1 Frontend](#61-frontend)
  - [6.2 Middle Layer](#62-middle-layer)
  - [6.3 Database Layer](#63-database-layer)
- [7. Implementation](#7-implementation)
  - [7.1 Detailed Implementation](#71-detailed-implementation)
  - [7.2 Frameworks & Libraries](#72-frameworks--libraries)
- [8. Database Techniques](#8-database-techniques)
  - [8.1 Indexing](#81-indexing)
  - [8.2 Prepared Statements](#82-prepared-statements)
  - [8.3 Transactions](#83-transactions)
- [9. Contributions](#9-contributions)

---

## 1. Title: GradXplorer
**Navigate academia, discover top schools, and find leading researchers with ease!**

All the code used is available in this repository.

This Dash app is now deployed on PythonAnywhere: [GradXplorer](https://cs411-ningyuanxie.pythonanywhere.com/)

---

## 2. Purpose

### 2.1 Application Scenario
Students preparing for graduate school applications often struggle to find reliable information on:  
- **Top universities** in their fields of interest
- **Leading researchers** and their affiliated institutions
- **Trending academic topics** shaping research today

**GradXplorer** centralizes this information, making it accessible and easy to explore.

### 2.2 Target Users
- **Undergraduate students** considering graduate school
- **Researchers** seeking collaboration opportunities
- **Professionals** exploring further education in academia

### 2.3 Objectives
- **Guide** users in identifying the best academic programs for their field.
- **Highlight** influential researchers and their contributions.
- **Assist** potential scholars in understanding the current trending topics.

Stay ahead in the academic world with **GradXplorer**!


---

## 3. Demo
**Link to video demo:** [Demo Video](https://mediaspace.illinois.edu/media/t/1_gezkl50u)


---

## 4. Installation
Only the given datasets (MySQL from MP4, MongoDB from MP3, and Neo4j from MP3) are used for this application. For environment setup, there are two options available below. After installation, simply run the third command below to start the application.

### 4.1 Option 1: Install Packages from requirements.txt
```bash
pip install -r requirements.txt
```

### 4.2 Option 2: Use Conda Environment from environment.yml
```bash
conda env create -f environment.yml
conda activate cs411
```

### 4.3 Run the Application
```bash
python app/app.py
```


---

## 5. Usage
**GradXplorer** provides an intuitive web-based interface for exploring academic data. After running the main entry file `app.py`, the application will launch at [http://0.0.0.0:8050/](http://0.0.0.0:8050/).

### 5.1 Widget 1: Top 10 Trending Keywords in Publications
This widget provides a high-level view of research trends by displaying the top 10 trending keywords for a selected year. 

Users can select a database source and adjust the slider to specify a year; the widget will update dynamically.

Database **MongoDB** and **MySQL** are used to store and retrieve this data.

### 5.2 Widget 2: User-selected Favorite Keywords
After viewing the top 10 trending keywords, users can configure their favorite keywords from the list for further research. 

By default, the selection includes five pre-selected AI-related themes, and users can add or remove favorite keywords as needed. In addition, a pie chart will reflect the current selection in real-time.

This control widget will also change the input keywords selection for **Widget 3** and **Widget 4** below.

Database **MySQL** is used to store and retrieve this data.

### 5.3 Widget 3: Faculty Members Relevant to Selected Keywords
Using the configured keyword selection dynamically generated by **Widget 2**, this widget lists all relevant faculty members and their affiliated universities.

Users can also **delete or restore faculty records** in the backend database during research and selection. A separate viewer provides a real-time count of faculty members currently in the database, reflecting these changes dynamically. These updates will impact the backend database **MySQL**.

Database **MySQL** is used to store and retrieve this data.

### 5.4 Widget 4: Faculty Members with the Highest KRC for Selected Keywords
This widget helps identify top faculty members based on their **Keyword-Relevant Citation (KRC) score**. 

Users can choose from one of the two database sources, select a pre-selected keyword dynamically generated by **Widget 2**, and pick a university to view faculty with the highest KRC scores in the selected field.

Database **MongoDB** and **MySQL** are used to store and retrieve this data.

### 5.5 Widget 5: Top 10 Keywords in Faculty Interests from Selected Universities
This widget identifies the most common research keywords among faculty at a specific university, complementing the insights from **Widget 1**.

Users can also **delete or restore keywords** in the backend database, with real-time updates displayed in a separate viewer that tracks the number of keywords currently stored in the database. These updates will impact the backend database **Neo4j**.

Database **Neo4j** is used to store and retrieve this data.

### 5.6 Widget 6: Top 10 Universities Collaborated with Selected Universities
This widget visualizes institutional research collaborations. Specifically, users can select a university to view its **top 10 collaborating institutions** including the number of coauthors, helping users understand academic partnerships and research networks.

Users can also click on the generated sunburst chart to view basic information about the selected university, including its name, total faculty count, and logo if available. For universities with strong CS programs, the coauthor ratio (number of coauthors / total faculty count) among their collaborating institutions is usually higher.

Database **Neo4j** and **MySQL** are used to store and retrieve this data.

---

## 6. Design

**GradXplorer** follows a **dashboard-driven** approach using Dash Plotly to present interactive data visualizations. Overall, the application is structured using a **three-tier architecture**:

### 6.1 Frontend (Dash Plotly)
Provides an interactive user interface with data filtering and visualization features. This is achieved through a combination of **six** Dash widgets, including bar charts, pie charts, data tables, and sunburst charts. They are arranged in a rectangular layout that is aesthetically pleasing and easy to navigate.

### 6.2 Middle Layer (Flask & Python)
Handles business logic, processes user requests, and acts as an intermediary between the frontend and the database. More details on the middle layer can be found in the **Implementation** section.

### 6.3 Database Layer (MySQL, MongoDB, Neo4j)
Stores relational, document, and graph Academic World data for efficient retrieval. Specifically: widget 1 and 4 use **MongoDB**; widget 1, 2, 3, 4, and 6 use **MySQL**; widget 5 and 6 use **Neo4j**.


---

## 7. Implementation

### 7.1 Detailed Implementation

- `app.py` – Main entry point for the application, structured using standard Dash app logic: app initialization, database initialization, layout setting, callback functions, and application execution. Most functions are defined in separate files to follow good software engineering practices and modularization.
- `mysql_utils.py` – Defines functions to connect and disconnect from MySQL, as well as functions for querying, deleting, and restoring backend data.
- `mongodb_utils.py` – Defines functions to connect and disconnect from MongoDB, along with functions for querying backend data.
- `neo4j_utils.py` – Defines functions to connect and disconnect from Neo4j, as well as functions for querying, deleting, and restoring backend data.
- `layout_utils.py` – Defines various Python classes to construct configurable widgets for reusability, including `GraphWidget`, `ControlWidget`, `TableWidget`, `CountDisplayWidget`, `DeleteWidget`, `RestoreWidget`, and `RefreshWidget`.
- `layout.py` – Uses self-defined widget classes to structure the actual application layout; also provides various IDs for each widget.
- `callbacks-utils.py` – Contains helper functions for various graphing functions used in `callbacks.py`, including `create_bar_chart`, `create_pie_chart`, `create_data_table`, `create_sunburst_chart`, `create_section_header`, and `create_info_table`.
- `callbacks.py` – Applies the callback decorator to the functions from the aforementioned `*_utils.py` files, linking them to the corresponding widgets through widget IDs from `layout.py`. This file contains all the callback functions that handle user interactions and update the dashboard accordingly.


### 7.2 Frameworks & Libraries
- **Pandas** – Used for DataFrame operations necessary to plot certain graphs.
- **Dash Plotly** – Used for building interactive dashboards.
- **Flask** – Handles backend logic and API requests.  
- **Database-related Frameworks** – Libraries used to interact with backend databases: `mysql.connector`, `pymongo`, and `neo4j`.


---

## 8. Database Techniques

### 8.1 Indexing
- `mongodb_utils.py` constructs indexes to optimize query performance for the following two functions:
  - `find_most_popular_keywords_mongo(year)`
  - `find_top_faculties_with_highest_KRC_keyword(keyword, affiliation)`
- `mysql_utils.py` also constructs indexes to optimize query performance for the following function:
  - `find_most_popular_keywords_sql(year)`

### 8.2 Prepared Statements
- `mysql_utils.py` applies prepared statements to prevent **SQL injection attacks** by separating query logic from input parameters:
  - `find_faculty_relevant_to_keyword(keyword: str)`

### 8.3 Transactions
- `mysql_utils.py` uses transactions during deletion and restoration to ensure **atomicity**, maintain **data consistency**, and provide **isolation**:
  - `delete_faculty(faculty_id)`
  - `restore_faculty()`
- `neo4j_utils.py` also uses transactions during deletion and restoration for the following functions:
  - `delete_keyword(keyword_id)`
  - `restore_keyword()`


---

## 9. Contributions
**This app was solely developed by Ningyuan Xie.**
