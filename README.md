
Project Organization
------------

    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── intermediate   <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data set.
    │   └── raw            <- The original, immutable data dump.
    ├── notebooks	   <- Jupyter notebooks. 
    └── src                <- Source code for use in this project.
        ├── __init__.py    <- Makes src a Python module
        ├── client.py      <- Any external connection (via API for example) should be written here    
        ├── params.py      <- All parameters of the execution
        │
        └── etl         <- Scripts to containing each step of the ETL process.
    ├── requirements.txt   <- The requirements file for reproducing the enviroment, generated with `pip freeze > requirements.txt`
         
--------


# Workshop 2: ETL process using airflow
By **Natalia López Gallego**

## Overview

During this workshop, the Spofy dataset was sourced from a CSV file, processed using Python and Airflow to perform necessary transformaons, and subsequently loaded into a database. At the same me, the Grammys dataset was uploaded to a database, with Airflow employed to retrieve data from it. Furthermore, Airflow facilitated the extracon of audio features from an audio source via the Recocobeats API. Transformaons were applied across all three datasets, which were then merged and loaded into a database as well as the Drive associated with a service account on Google Cloud Plaorm (GCP).

Technologies utilized in this project include:

- Airflow: For ETL pipeline orchestrating.

-  ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54): For data handling and analysis.
    
-    
   ![Jupyter Notebook](https://img.shields.io/badge/Jupyter%20Notebook-F37626?style=flat-square&logo=jupyter&logoColor=white): For interactive data analysis and visualization.
-   ![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white): For database management.


## Table of Contents




## Prerequisites  

Before you begin, ensure you have met the following requirements:
- [![Windows](https://custom-icon-badges.demolab.com/badge/Windows-0078D6?logo=windows11&logoColor=white)](#) Windows 10 version 2004 and higher (Build 19041 and higher) or Windows 11
-    [![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=fff)](#): 3.12.9
- [![Visual Studio Code](https://custom-icon-badges.demolab.com/badge/Visual%20Studio%20Code-0078d7.svg?logo=vsc&logoColor=white)](#) or your prefered Python IDE.



## Installing WSL 2 and Docker for PostgreSQL Deployment

WSL 2 (Windows Subsystem for Linux 2) provides a lightweight, virtualized Linux environment that integrates seamlessly with Windows, enabling developers to run Linux-based tools and applications with improved performance and compatibility. Using a Dockerized MySQL image within WSL 2 allows for consistent, isolated, and portable development environments.

### Enabling WSL 2

1. Open PowerShell as Administrator.
2. Run:
    ```bash
    wsl --install
    ```
3. Set WSL 2 as the default version:
    ```bash
    wsl --set-default-version 2
    ```

### Installing Ubuntu

1. Run the following command in PowerShell:
    ```bash
    wsl.exe --install -d Ubuntu-24.04
    ```
2. Launch Ubuntu from the Start menu and complete the installation by creating a new user account.

### Turning on Docker Desktop WSL 2

Important: Uninstall any previous versions of Docker Engine and CLI installed through Linux distributions.

1. Download and install the latest Docker Desktop for Windows.
2. Follow the installation instructions and enable WSL 2 when prompted.
3. Start Docker Desktop.
4. Navigate to **Settings > General** and select **Use WSL 2 based engine**.
5. Click **Apply & Restart**.

### Confirming Docker Installation

1. Open a WSL distribution (Ubuntu-24.04).
2. Display the version and build number by entering:
    ```bash
    docker --version
    ```
3. Test the installation by running a simple built-in Docker image:
    ```bash
    docker run hello-world
 
    ```
## Redash setup

Redash is an open-source data collaboration platform that enables you to connect to any data source, visualize data and share it.

### Cloning the Repository

We are going to self-host Redash using the official setup script. For this, you need to clone the Redash repository in your *WSL 2 Ubuntu 24.04* machine.

```bash
	git clone https://github.com/getredash/setup.git etl-ws-  	2/redash
	cd etl-ws-2/redash
 ```

This will clone the repository into a directory named `redash` (already existent inside this project’s directory) and change into that directory.


### Installation

When running the Redash setup script (`setup.sh`), you might encounter the following error:

``./setup.sh: 187: pwgen: not found``

This error indicates that the `pwgen` utility is missing. To fix this, install `pwgen` on your system.

Run:

``` bash
sudo apt update && sudo apt install -y pwgen
```

After installing `pwgen`, re-run the setup script in the `pwgen` directory:

``` bash
./setup.sh
```

### Mail Configuration (optional)

To enable Redash to send emails (e.g., for alerts or password resets), you must configure your SMTP settings. Depending on your installation method, these environment variables might reside in a `.env` file (e.g., `/opt/redash/.env`).

Add the following environment variables, replacing the placeholder values with your actual SMTP server details:

```bash 
REDASH_MAIL_SERVER=your_smtp_server_address
REDASH_MAIL_PORT=your_smtp_port
REDASH_MAIL_USE_TLS=true_or_false
REDASH_MAIL_USE_SSL=true_or_false
REDASH_MAIL_USERNAME=your_smtp_username
REDASH_MAIL_PASSWORD=your_smtp_password
REDASH_MAIL_DEFAULT_SENDER=your_default_sender_email
```

**Important:**

-   Set `REDASH_MAIL_USE_TLS` to `true` if your SMTP server requires TLS.
    
-   Set `REDASH_MAIL_USE_SSL` to `true` if your SMTP server requires SSL.
    
-   Do not set both TLS and SSL to `true` simultaneously.
    

After updating your mail configuration, restart your Redash services to apply the changes (`docker-compose up -d`, running `docker-compose restart` won’t be enough as it won’t read changes to env file). To test email configuration, you can run `docker-compose run --rm server manage send_test_mail`.

## Usage 

### Running a PostgreSQL Instance with Docker Compose

We will use a single container for our PostgreSQL instance with Docker Compose. In your command line or terminal of your WSL2 machine, navigate to this project's directory, and into the `postgresql` directory.

```bash
cd etl-ws-2/postgresql
```

The PostgrSQL instance works with a `docker-compose.yml` file in the following way:


```bash
services:
  postgres_db:
    image: postgres:latest
    container_name: pg
    ports:
      - '5433:5432'
    secrets:
      - postgres_user
      - postgres_password
    environment:
      POSTGRES_USER_FILE: /run/secrets/postgres_user
      POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password
      POSTGRES_DB: ws_002
    volumes:

      - ./my_db:/var/lib/postgresql/data

volumes:
  my_db:

secrets:
  postgres_user:
    file: ./postgres_user.txt # Create this file with your user
  postgres_password:
    file: ./postgres_password.txt # Create this file with your password

```

1.  **Database Initialization**:
    
    -   When the container starts, PostgreSQL reads the username and password from the secrets files (`postgres_user.txt`  and  `postgres_password.txt`).
        
    -   It creates a database named  `ws_002`  using the provided credentials.
    
2.  **Data Persistence**:
    
    -   The database data is stored in the local directory  `./my_db`  on the host machine, ensuring it persists across container restarts or deletions.
        
3.  **Accessing the Database**:
    
    -   External applications can connect to the database using  `localhost:5433`  (or the host's IP address) with the credentials specified in the secrets files.

### **Steps to Use:**

1.  Create the secrets files in the `postgresql` directory:
    
    -   `postgres_user.txt`: Add the database username (e.g.,  `admin`).
        
    -   `postgres_password.txt`: Add the database password (e.g.,  `password123`).

    -   Both these files are to be protected with `chmod 400` (Read-only for the owner user).

2. Create the volume directory in the  `postgresql` directory :

	  ```bash
	 mkdir my_db
	```

	##### **Example Directory Structure:**
	├── docker-compose.yml
	├── postgres_user.txt
	├── postgres_password.txt
	└── my_db/
        
3.  Run the Docker Compose file:
    
	  ```bash
	 docker-compose up -d
	```
   
4.  Access the PostgreSQL command-line client:
	```yaml
	docker exec -it pg psql -U <your_user> -d ws_002 -p 5433
	```
### Setting up a .env file for PostgreSQL Credentials in WSL2 Ubuntu 24.04

A `.env` file is needed to store your PostgreSQL credentials securely, including the WSL2 IP address and the password  set up.

**1. Locate the project directory:**

Navigate to the directory where this repository has been cloned This is where you'll create the `.env` file. In the terminal it can be be done trhought the following commands:
```
    cd /path/to/cloned/repository/directory
```
**2. Create the .env file:**

In the project directory, create a new file named `.env` (no file extension). You can do this from the command line:
```
touch .env
```

Or using a text editor.

**3. Add your PostgreSQL credentials to the .env file:**

Open the `.env` file with a text editor and add the following lines, replacing the placeholders with your actual values:

```
	PG_USER=your_postgres_user
    PG_PASSWORD=your_postgres_user_password
    PG_HOST=your_wsl2_ip_address
    PG_PORT=5433
    PG_DATABASE=ws_002
```
-   **`PG_USER`:** Your PostgreSQL username.
-   **`PG_PASSWORD`:** The password you set for your PostgreSQL user.
-   **`PG_HOST`:** This is _crucial_. You need the IP address of your WSL2 instance. See step 4 below to find this.
-   **`PG_DATABASE`:** The PostgreSQL database created with the docker compose file.
-   **`PG_PORT`:** The port MySQL is listening on port 5433 according to our docker compose file.

**4. Find your WSL2 IP Address:**

There are several ways to find the IP address of your WSL2 instance:

-   **From WSL:** Open your WSL2 terminal and run:
    
    Bash
    
    ```
    ip addr show eth0 | grep "inet\b" | awk '{print $2}' | cut -d/ -f1

    ```
    
-   **From Windows (PowerShell):** Open PowerShell as administrator and run:
    
    PowerShell
    
    ```
    wsl hostname -I
    ```
    
-   **From Windows (Command Prompt):** Open command prompt and run:
    
    ```
    wsl hostname -I
    ```
    

The output will be the IP address of your WSL2 instance. Use this IP address for `MYSQL_HOST` in your `.env` file.

**5. Secure the .env file:**

The `.env` file contains sensitive information. It's _extremely important_ to prevent it from being accidentally committed to version control (like Git). Add `.env` to your `.gitignore` file:

```
.env
```

This will tell Git to ignore the `.env` file.

## Using Redash

### Login to Redash

Once the setup is complete and the Redash services are running, you can access the Redash web interface using your browser. By default, the Redash instance will be available at:

http://localhost:5000/

Open this URL in your web browser to start using Redash.

### Connect to a Data Source

Before you can write queries, you need to connect Redash to a data source. Navigate to the 'Settings' and add your data source (select "PostgreSQL") with the appropriate credentials.

![Connect to a Data Source](https://redash.io/assets/images/docs/gitbook/add-data-source.gif)

### How to create a dashboard

A dashboard is composed of widgets, which can be any visualization created from the query source page. The dashboard is created by clicking on the “New Dashboard” button on the homepage, assigning it a name, and then clicking on the “save” button.

You can also, at any time, create a dashboard by clicking on the dropdown menu on the fixed navbar.

After this, you will have an empty page with the dashboard name. The next steps will explain how to create the widgets to fill the dashboard.

### Create query

Redash comes with an interface to write and run queries on the platform.

Just click on the “New Query” button, type a name for your query (otherwise, it will be considered a draft), copy and paste the query inside the text area, and click on the “save” button.

![Create query](https://redash.io/assets/images/docs/gifs/dashboards/dashboards.gif)

### Create visualizations for the query

All saved queries by default have a ‘Table’ visualization created. You can create more visualizations after the query runs for the first time.

The options are:

- Chart
- Cohort
- Counter
- Map
- And more.

Click on the “+ New Visualization” button, select the visualization type, set a name and options for the visualization, and then click “save”.

Type the name of the query to see the visualizations available for the query.

Choose the visualization, optionally set the widget’s size (Regular or Double), and click the “Add to Dashboard” button.

![Create visualizations for the query](https://redash.io/assets/images/docs/gifs/visualization/new_viz.gif)

## Airflow

With the virtual enviroment activated install airflow.
```
pip install apache-airflow
```

This will create an `airflow` directory with a `dags` directory  into where you should clone this repository.

```bash
    cd git clone https://github.com/ntlg72/ws_002.git
  ```
    

## Python Virtual Environment & Dependencies

### Implementation

1. Create a virtual environment (it must not be inside `dags` directory):
    ```bash
    py -m venv <environment_name>
    ```
2. The invocation of the activation script is platform-specific (`_<venv>_` must be replaced by the path to the directory containing the virtual environment):

```markdown
| Platform | Shell      | Command to activate virtual enviroment |  
|----------|------------|----------------------------------------
| Windows  | cmd.exe    | C:\> <venv>\Scripts\activate.bat       |     
|          | PowerShell | PS C:\> <venv>\Scripts\Activate.ps1    |    
```

3. The project directory contains a `requirements.txt` file listing all necessary dependencies. To install them, wihile the virtual enviroment is activated, run:
    ```bash
    pip install -r ~/airflow/dags/ws_002/requirements.txt
    ```
   You can check the installed dependencies using:
    ```bash
    pip list
    ```

## Run the notebooks (before starting Airflow)  

The notebook to load the necessary data (`1.0-nlg-load-grammys-to-db.ipynb`) into the database should be executed before starting Airflow.


## Starting Airflow

With the activated virtual enviroment run:

```bash
airflow standalone
```

Open [http://localhost:8080](http://localhost:8080) and use the credentials generated in the terminal to login.

## GCP Google Drive API

You can follow the following guide for configuring the Google Drive API and a creating service account on GCP to obtain a key:https://medium.com/data-science/how-to-upload-files-to-google-drive-using-airflow-73d961bbd22

## Setting Up Google Drive in Airflow

To integrate Google Drive with Airflow, follow these steps:

#### 1. Add Google Drive Connection

-   Navigate to the **Airflow UI** and click on **Admin > Connections**.
-   Click on **Create** to add a new connection.
-   Fill in the required fields:
    -   **Conn Id**: Provide a unique identifier, e.g., `google_drive`.
    -   **Conn Type**: Choose `Google Cloud` from the dropdown.
    -   **Key File Path**: Enter the file path where your service account key (JSON) file is stored. For example: `/path/to/keyfile.json`.
    -   **Scope**: Set the scope to enable permissions to `https://www.googleapis.com/auth/drive`.
 

#### 2. Configure the Key File

-   Obtain the Google Drive service account key as a JSON file from the Google Cloud Console.
-   Save the key file in a secure location accessible by your Airflow instance.

#### 3. Set the Environment Variable (Optional)

-   To avoid hardcoding the key file path, set an environment variable in your system:
    
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"
    
    ```

## Manually Trigger the DAG 

If you want to trigger the `etl_full_pipeline_dag` manually:

-   Open the **Airflow UI**.
-   Navigate to the DAGs list and find `etl_full_pipeline_dag`.
-   Click on the **Trigger DAG** button.
    

> Written with [StackEdit](https://stackedit.io/).
