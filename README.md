# note-flow

Run common DE/ML pipelines using notebook tasks leveraging Airflow. 

### Steps:

- From the `airflow/dock` directory, build the docker file on your local machine. <p>
``` docker build . -f Dockerfile --pull --tag note-flow:0.0.1 ```

- From **airflow/** directory .. Run `docker-compose up` to start airflow with docker
- Paste your new dags into **airflow/dags/** and corresponding notebooks into **airflow/dags/notebooks/**
- Local airflow web can be accessed from `localhost:8080` to run the workflows.

    
