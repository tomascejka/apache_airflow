# Building your first workflow

zdroj - https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html

## Keywords

**Dag**

- Dag is a collection of tasks organized in a way that reflects their relationships and dependencies
- It’s like a roadmap for your workflow, showing how each task connects to the others
- has unique id, call as `dag_id`
- lze jej dokumentovat viz https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html#adding-dag-and-tasks-documentation

**Task**

- each DAG has 1:N tasks
- arguments can be default defined per each task (to each operator);
- arguments can be overrided per task (during operator inicialization)
- lze jej dokumentovat viz https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html#adding-dag-and-tasks-documentation
- tasky mohou mit dependency, napr. mezi sebou viz [priklad](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html#setting-up-dependencies)
	- Be mindful that Airflow will raise errors if it detects cycles in your Dag or if a dependency is referenced multiple times.

**Operator**

- operator represents a unit of work in Airflow
- each task must have unique id, call `task_id`
- building blocks of your workflows, allowing you to define what tasks will be executed
- we can use operators for many tasks
- To use an operator, you must instantiate it as a task
- Tasks dictate how the operator will perform its work within the Dag’s context
- operator spousti napr. bash, via atribute bash_command
- bash command lze zadat formou sablony, 
	- tzv jinja for templating viz https://jinja.palletsprojects.com/en/2.11.x/
	- viz tutorial https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html#using-jinja-for-templating
	

## Prerekvizity

Instalace Airflow pres Docker viz [gud01_install_docker](../gud01_install_docker/README.md)

## Priklad: my_tutorial DAG

Soubor `dags/tutorial.py` - 3 BashOperator tasky (print_date -> sleep, templated).
Detaily viz [ANA-02](analyses/ANA-02_tutorial_dag.md)

## Changelog

- 2026-06-12: Docker setup Airflow 3.2.2, tutorial DAG uspesne spusten

## Zdroje

1. https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-production-docker-images
2. https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html
3. https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html