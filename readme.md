## Run locally

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db init
airflow standalone
```

Data Source: 
https://tfl.gov.uk/info-for/open-data-users/our-open-data?intcmp=3671#on-this-page-2

API Documentation: 
https://content.tfl.gov.uk/tfl-live-bus-river-bus-arrivals-api-documentation.pdf

List of APIs: 
https://api-portal.tfl.gov.uk/apis

Alternative Public Real-Time Datasets: 
https://github.com/bytewax/awesome-public-real-time-datasets?tab=readme-ov-file