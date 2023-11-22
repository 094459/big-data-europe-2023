## Demo Setup

Run up a local MySQL database which has the following connection settings

```
docker run --name mysql -d \
   -p 3306:3306 \
   -e MYSQL_ROOT_PASSWORD=change-me \
   --restart unless-stopped \
   mysql:8
```

You can connect to this MySql server running on 127.0.0.1:3306, connecting as root with a password of change-me. You can also create a connection from within VSCode.

> *Tip!* If you get errors such as "ERROR 2002 (HY000): Can't connect to local MySQL server through socket '/tmp/mysql.sock' (2)", use the "--protocol tcp" 

You will need to enable access to your MySQL database from Airflow. Running this worked for me. **Remember, this is only playing around/demo, never never ever do this for something in real life**

```
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'change-me';
SET GLOBAL local_infile=1;
FLUSH PRIVILEGES;
```

Start MWAA by running the following command from within the aws-mwaa-local-runner folder

```
./mwaa-local-env start
```

And after a few minutes, you should be able to connect to the localhost on port 8080, logging in as admin/test.

The folders you will be working with are as follows

```
├── README.md  <-- this file
├── aws-mwaa-local-runner
│   ├── db-data <--the postgresql database that mwaa-local-runner will use, spinning up as a container
│   ├── mwaa-local-env <-- the control script
│   ├── docker
│   │   ├── Dockerfile
│   │   ├── config
│   │   │   ├── airflow.cfg <-- the Apache Airflow configuration file, modified to hot load DAGs every 5 sec
│   │   │   ├── constraints.txt
│   │   │   ├── mwaa-base-providers-requirements.txt
│   │   │   └── webserver_config.py
│   │   │   └── .env.localrunner <-- where you define env variabls, including things like AWS Credentials that are passed into the Docker containers
│   │   ├── docker-compose-local.yml <-- the core Docker Compose file used when starting a local mwaa-local-runner
│   │   ├── docker-compose-resetdb.yml
│   │   ├── docker-compose-sequential.yml
│   │   └── script <-- key scripts used to boot Airflow and setup AWS stuff
│   │       ├── bootstrap.sh
│   │       ├── entrypoint.sh
│   │       ├── generate_key.sh
│   │       ├── run-startup.sh
│   │       ├── shell-launch-script.sh
│   │       ├── systemlibs.sh
│   │       └── verification.sh
│   └── startup_script
│       └── startup.sh
└── workflow <-- this is where all the working files are sourced (DAGs, requirements.txt, etc)
    ├── dags
    │   ├── example_dag_with_taskflow_api.py
    │   ├── info-environment.py
    │   └── jokes.py
    ├── plugins
    └── requirements
        └── requirements.txt
```

**Setting up Airflow stuff**

You need to set up the following in the default_aws connection, under the extras add the following based on your aws region

{"region_name": "us-east-1"}

You need to configure the default_mysql as follows

```
hostname - host.docker.internal
database - jokes
login - root
password - change-me
port - 3306
Extras - {"local_infile": "true"}
```

## To stop the demo

Stop the running MWAA local runner (CTRL + C) and then check to make sure there are no dangling containers.

Stop the MySQL database using the following command

```
docker kill {container image}
```