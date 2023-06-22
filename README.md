## Data Engineering Project 1 / COVID-19 Dataset

This project will explore and dissect the data regarding the novel coronavirus (2019-nCoV) from various health organizations. The aim is to answer a series of critical questions and assess the progression of the pandemic over a specified period of time.

## 1. Description of the Problem:

COVID-19, first identified in Wuhan, the capital city of China's Hubei province, has dramatically altered the global health landscape. People began to develop a form of pneumonia of unknown cause, against which existing vaccines and treatments were ineffective. Furthermore, the virus showed signs of human-to-human transmission, and the rate of infection seemed to increase significantly in mid-January 2020. As of January 30, 2020, approximately 8,243 cases have been confirmed. This situation demands rigorous assessment and continuous monitoring of the pandemic's evolution.

<img width="600" height="400" alt="image" src="https://github.com/Irf4n-Muhammad/Data-Engineering-Project_COVID19-Dataset/assets/121205860/c067e3a7-1ef0-49bf-b46d-67273295282e">

This scanning electron microscope image shows SARS-CoV-2 (yellow)—also known as 2019-nCoV, the virus that causes COVID-19—isolated from a patient in the U.S., emerging from the surface of cells (blue/pink) cultured in the lab.

Credit: NIAID-RML

There are several questions to be answered:

1. What's country has the most confirmed COVID-19 patient?
2. What's the death probability of the COVID-19 patient?
3. What's region(continent) has the least number of COVID-19 patient?
4. What are factor that impact the spread of COVID-19 virus?
5. What's country has the highes probability of getting infected by COVID-19?

## 2. Objective:

In this context, data becomes an indispensable tool. Governments and health organizations around the world have been recording data, which can be used to better understand the virus and its spread. However, data needs to be properly structured and easily analyzable to facilitate rapid and informed decision-making. Therefore, we will develop a data pipeline to streamline the process from data ingestion to data visualization.

## 3. Technologies:

The choosen technologies is variative and depends on the case and condition. There are some option for certain case and in this case, I am using this option since it's the easiest.

- Dockerfile
- Docker Compose
- VM GCP
- Airflow / Prefect
- GCS (Google Cloud Storage)
- Bigquery
- DBT cloud / Spark
  DBT is best for this case for several reason:
  1. The file's size is small (2 GB)
  2. DBT provide the docuementation
- Google Data Studio

## 4. Data Architecture:
<img width="700" alt="image" src="https://github.com/Irf4n-Muhammad/Data-Engineering-Project_COVID19-Dataset/assets/121205860/6f76020a-5b27-4935-9878-af6bd7d388b3">


## 5. Data Description:

The datasets that we'll be using for this analysis are comprehensive and varied, offering different granularities of data. These are as follows:

- full_grouped.csv: This dataset provides daily updates on the number of COVID-19 cases. The unique aspect of this data file is that it goes beyond the country level, incorporating the number of cases at the county, state, and province level. This comprehensive breakdown allows us to analyze not only the overall impact on a given country but also how the virus is affecting specific regions within those countries.

- covid_19_clean_complete.csv: Like the previous dataset, this also provides day-to-day updates on the number of COVID-19 cases per country. However, unlike full_grouped.csv, it does not offer data at the county, state, or province level. This dataset can be used for a broad overview of the impact on each country as a whole.

- country_wise_latest.csv: This dataset offers the most recent snapshot of the number of COVID-19 cases on a country level. It is useful for getting an up-to-date overview of the current situation in each country.

- day_wise.csv: This dataset offers a daily summary of the number of COVID-19 cases. However, it does not provide country-level data. This global overview can provide insights into the overall trends and pace of the pandemic.

- usa_county_wise.csv: This dataset provides a granular look at the day-to-day number of COVID-19 cases at the county level within the United States. This allows for a detailed understanding of how the pandemic is affecting different regions within the US.

- worldometer_data.csv: The final dataset is sourced from Worldometer, a reputable reference website that provides real-time statistics for a wide variety of topics. This dataset provides the most recent data on the number of COVID-19 cases and can offer valuable insights into the current state of the pandemic.

These data was collected from kaggle : https://www.kaggle.com/datasets/imdevskp/corona-virus-report

## 6. Set Up the Environment

You gonna need some tools:

1. Google Cloud Platform
2. Terraform

### 6.1 Google Cloud Platform:

<img width="700" alt="image" src="https://github.com/Irf4n-Muhammad/Data-Engineering-Project_COVID19-Dataset/assets/121205860/5424e67f-d94a-45fa-8ab9-ac706aeddfad">

In this chapter, we will set up several things you need to set up your firest google cloud platform accout before ready to be used

1. Create the new project (you can use the old project, if you think it's fine)
2. Set the VM Instances
3. Set the service account and assign the right roles on it
4. Create the new bucket
5. Create the new dataset (optional, due to you can make it along the process)

### 6.2 Terraform:

<img width="700" alt="image" src="https://github.com/Irf4n-Muhammad/Data-Engineering-Project_COVID19-Dataset/assets/121205860/2faa8507-d149-4bc5-889d-9381c84f21be">

The terraform will help us to make the stable and organizable environment for our google cloud and it's very easy to monitor since we only use python file to control. You can even share it to other team member if you work in group, so you all can assure using the same environment set up.

1. Set the main.tf
2. Set the variable.tf
3. Run the terraform through some command :
   1. terraform init = initialize the terraform
   2. terraform validate = check and validate that it's proper update
   3. terraform plan = you can see what's the update and what's new in your environment
   4. terraform apply = run and make the update

## 7. Airflow:

<img width="700" alt="image" src="https://github.com/Irf4n-Muhammad/Data-Engineering-Project_COVID19-Dataset/assets/121205860/0b299387-a421-49b7-aa4c-97c7816b37a9">

Airflow is a tool to help the data engineer to monitor the ingesting data process. We can create the workflow by our own using python file and we can edit as like as we want. Here are some sets up to run the Airflow:

1. Prepare the Dockerfile (check the environment that will be used for ingesting the data)
2. Prepare the docker-compose.yaml (fill some specific variable based on your personal data)
3. Prepare .env file
4. Prapare the requirements.txt file
5. Open the virtual machine and connect to the host (you can find it on vs code)
6. Set the port (8080), make sure there is no machine using the same port
7. Run the docker-compose build in directory that has docker-compose.yaml and Dockerfile
8. Run the docker-compose up airflow-init to initialize the airflow
9. Run the docker-compose up, wait until all the image has settled
10. Open the search engine (google, bing, etc) and open the web ( localhost:8080 )
11. Wait until the loading ended
12. Sign in the airflow using the password and username that you set before (in docker-compose.yaml)
13. Choose the DAG file that you want to run
14. Trigger the DAG
15. If all task has dark green color (which means succeed), then please check your gcs bucket or bigquery (only if your DAG file has a task sending the file to the bgquery)
16. If there is an error, then click the square task and click log button to see what's the message giving to you the error information
    There are several thing that I ever experienced that cause an error:
    1. Airflow version is too low - Solution = Set the latest version in your Dockerfile and when you run the docker-compose build
    2. DAG issue - Solution = Since it would be very specific, so please check the log to see what's error there
    3. PORT is occupied - Solution : If you're using the docker, then you can find out what's machine that may use that port and you can delete that image in the docker (you can use docker apps or type the command in the git bash)

## 8. DBT:

<img width="700" alt="image" src="https://github.com/Irf4n-Muhammad/Data-Engineering-Project_COVID19-Dataset/assets/121205860/6104b129-366d-47c2-a3cb-7cda8357ac34">

DBT is the tool that you can transform your data using sql command. It's very simple and you can even create the documentation to track the history of the process and the dependencies of each files. To use DBT, you have two option to run it, either using local database or using dbt cloud. Each of them have their own benefits, but I suggest you to use dbt local since it's free.

Here's the way to set up the DBT:

1. Create the dictionary and clone your github repo
2. Run the dbt install ( pip install dbt-bigquery ), you can change the bigquery with other tools, for more information check the dbt website
3. Set the profiles.yaml to set the information you need for initialization
4. Run ( dbt init ) to initialize
5. Run ( dbt debug ) to check if it's successful
6. Try to run the dbt ( dbt run ), make sure in the same directory where dbt_projects.yaml exist
7. In the model folder, create new dir (staging) and (core)
8. In the staging dir, create new file (schema.yaml and <your-file.sql>)
9. Write your database in the schema.yaml and create the model in your-file.sql
10. Use macros if you need to create function
11. Set the packages if you need that and run ( dbt deps )
12. Run the file using ( dbt run ) and check your bigquery table and see if the table has created

## 9. Google Data Studio:

<img width="700" alt="image" src="https://github.com/Irf4n-Muhammad/Data-Engineering-Project_COVID19-Dataset/assets/121205860/5e3bd7a1-eee6-43a8-8912-985be393722e">

It's pretty simple, you can connect your bigquery with google data studio and use the created table to be visualized. Build your dasboard which with the hope, it can answer all the problem solving question clearly.

These are my dashboard:

<img width="700" alt="image" src="https://github.com/Irf4n-Muhammad/Data-Engineering-Project_COVID19-Dataset/assets/121205860/82b1b5ae-6632-4b11-ae01-bda45ccc8df2">
<img width="700" alt="image" src="https://github.com/Irf4n-Muhammad/Data-Engineering-Project_COVID19-Dataset/assets/121205860/2a61fa8c-7de7-4759-80a5-0c16b90a785a">

From this two data visualization, we could answer our question:

#### 1. What's country has the most confirmed COVID-19 patient?

Answer: (Graph 1) America, with 27.1% confirmed COVID-19 patient in the world are from America.

#### 2. What's the death probability of the COVID-19 patient?

Answer: (Graph 1) We could see from the graph, the increase of the death and confirmed patient has the same pattern. So we could calculate it to be:
Death Ratio = Total death / Confirmed patient x 100 = 43.384.904 / 828.508.482 x 100 = 5.236 %

#### So the death probability of COVID-19 patient is 5.236 %

#### 3. What's region(continent) has the least number of COVID-19 patient?

Answer: (Graph 1) From the map, we could say Africa and Australia has smaller number compared to other continents. However, We still has shortage of data to get the reason why.

#### 4. What are factor that impact the spread of COVID-19 virus?

Answer: (Graph 2) The graph shows us the ratio of the covid-19 patient by the whole population is generally has higher position for small population country. It means that most of the people in small country has higher probability to get infected rather than the big country, even though the number of patients is still less than big country.

#### 5. What's country has the highes probability of getting infected by COVID-19?

Answer: (Graph 2) Qatar position in the first place with 4% probability. It's pretty high comparing to other countries, which the second position only have 2% probability.

Those are the question we can answer from our graph. However, there are so many information we haven't answered yet, so you can dig more information from that graph by yourself.

## Reference Link:

https://www.niaid.nih.gov/news-events/novel-coronavirus-sarscov2-images

https://blogs.cdc.gov/publichealthmatters/2019/04/h1n1/
