# Big Data Trend Metrics Project 

An Integrated Big Data Solution for Real-Time Monitoring and In-Depth Analysis of news Trends.
This system is designed for real-time analysis of news feeds and visualization of commonly used words and expressions. It utilizes a wide range of technologies for various tasks, from data streaming to machine learning and analytics.

## Tech Stack
- Apache Kafka
- Apache Spark Streaming
- Spark NLP Machine Learning Models
- Cassandra
- Apache Airflow
- Docker
- Prometheus
- Grafana

## Infrastructure
The system is deployed on an Ubuntu 22.04 VPS server with 12 cores, 60 GB RAM, and 2 TB SSD. This server provides the resources necessary for all system components. The server architecture allows for further horizontal scaling.

## Project Structure:

```
trend_metrics/
├── README.md
├── .gitignore
├── .dockerignore
└── airflow/
├── docker-compose.yml
├── .dockerignore
└── cassandra/
├── docker-compose.yml
├── .dockerignore
└── kafka/
├── docker-compose.yml
├── .dockerignore
└── spark/
├── docker-compose.yml
├── .dockerignore
└── monitoring/
├── docker-compose.yml
├── .dockerignore
└── scripts/
```

## Git
The project uses the Git version control system for versioning and secure development. Additional branches have been created for complex modules. The full project code is presented on Github.

## Docker
All project components are launched in 5 containers, but operate jointly in a single Docker network, ensuring isolation and the use of internal service names.

## Scripts
- `add_channels.py` - This script processes a list of news channels provided in the `feeds.csv` file. Channels are added manually to the `feeds.csv` by the user. Once the list is populated, `add_channels.py` adds these new channels to the Sqlite database. 
It's important to note that the actual list of channels used in production is not included in the GitHub repository for privacy and security reasons. Instead, a sample or template `feeds.csv` can be provided for reference. Additionally, the script sends metrics on the total number of channels to the Prometheus service.


- `feeds_to_kafka.py` - Reads the list of channels from the Sqlite database every hour, downloads available news from these channels, processing only newly received items, forms a Json message from each news item, sends it to the Kafka topic, and logs the last update time in Sqlite. The script also transmits metrics on the number of new messages in the last hour and the total number of messages over 24 hours.

- `keywords_day.py` - Groups and counts the number of key words from the Cassandra `keywords_hour` table and transfers the results for further analysis into `keywords_day` and `keywords_month` tables respectively.

## Apache Airflow
Controls the periodicity of script execution: 
- `AddChannels.py` updates the channel databases - once a day,
- `FeedsKafka.py` sends messages from news feeds to Kafka - every hour,
- `KeywordsDay.py` performs the merging and transferring of key words into the `keywords_day` and `keywords_month` tables - once a day.

Airflow also monitors the operation of all auxiliary scripts and sends a series of project metrics using the StatsD exporter to the Prometheus monitoring system.

## Apache Kafka
Receives messages in the `feeds` topic and ensures distributed data storage for subsequent processing. Kafka distributes all project messages in the topic across 3 partitions. The system scales effectively under increased load. For monitoring convenience, messages are stored in the topic for 24 hours. 
Example message in Kafka:

```json
{
"id": "3174ecee-f121-4150-8b39-49cbe2e21c94",
"title": "CNBC Daily Open: The Moody’s downgrade was a non-event",
"published": "2023-11-13T23:43:15Z",
"ChannelTitle": "CNBC Energy"
}
```

## Apache Spark
The Spark Streaming service connects to Kafka using the PySpark library and reads all incoming messages in real-time over an hourly interval. Data in binary form is then converted into a Spark dataframe, and all news headlines are combined into a single array for ease of processing. The prepared array of hourly news is sent to a pipeline for further processing using the Spark NLP library.

In the pipeline, data undergoes stages of:
- Tokenization - breaking down into unique tokens,
- Normalization - converting to lowercase and removing special signs and symbols,
- Stop-word removal using a pretrained model from the Spark NLP library,
- Lemmatization - converting words to their base form,
- Ngram generator - creating two-word phrases.

The output from the pipeline, unigrams and bigrams, are combined into a single Spark dataframe. Spark then groups the obtained words, counts them, and outputs a dataframe of the top 20 most mentioned words and expressions.

The processed dataframe is transformed into a suitable structure and sent to the database. Spark Streaming continues to operate in a launched state and processes the next hourly interval.

Spark's architecture consists of one Master node and two Worker nodes. Each Worker has 2 cores and 4 GB of RAM. The system scales both horizontally and vertically with increased load.

## Cassandra
Processed key words for the hourly interval are stored in the Cassandra database, which provides data replication and partitioning as the analyzed data volume increases.

Table structure `keywords_hour`:

```sql
CREATE TABLE IF NOT EXISTS keywords.keywords_hour (
hour text,
keyword text,
count int,
PRIMARY KEY (hour, count, keyword)
) WITH CLUSTERING ORDER BY (count DESC, keyword ASC);
```

Cassandra also has `keywords_day` and `keywords_month` tables, which store key words obtained by aggregating hourly data. With increasing data volumes, it is advisable to index the database and optimize SQL queries for performance enhancement.

## Prometheus
Collects key metrics in the project using StatsD Exporter:
- `channels_total` - total number of news channels,
- `kafka_messages_new` - number of new messages in the last hour,
- `kafka_messages_total` - total number of messages over 24 hours.

Prometheus transmits metrics to the metric visualization system via API.

## Grafana
Visualizes the received metrics in real time on a dashboard. 
Metrics visualized from Prometheus:
- Number of channels and messages metrics,
- Graph of the number of messages for each hour.

Metrics visualized from Cassandra:
- Top 20 most encountered key words in the last hour from the `keywords_hour` table. 
The list of key words is updated every hour.
For larger scale visualizations, the implementation can be achieved using REST API on PowerBi or Tableau dashboards.

## Demo Dashboard
The project's demo dashboard is available at:
[http://deeplogic.ch:4000](http://deeplogic.ch:4000/d/cf2b5d41-6049-4d61-a182-90099b833490/trendmetrics?orgId=1&from=1701971715821&to=1702058115821&theme=light)
