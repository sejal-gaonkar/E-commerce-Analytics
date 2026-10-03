\# E-Commerce Analytics - Real-Time Data Engineering Pipeline



\## Overview



This project implements a real-time e-commerce analytics pipeline that captures order events, processes streaming data, stores analytics data, and displays business insights through a dashboard.



The pipeline simulates an online shopping platform where customer orders and payments are generated continuously and processed in real time.



\---



\## Architecture

&#x20;           Order Producer

&#x20;                |

&#x20;                |

&#x20;                v

&#x20;           Apache Kafka

&#x20;                |

&#x20;                |

&#x20;                v

&#x20;     Spark Structured Streaming

&#x20;                |

&#x20;                |

&#x20;                v

&#x20;         PostgreSQL Database

&#x20;                |

&#x20;                |

&#x20;                v

&#x20;        Streamlit Dashboard

\---



\## Tech Stack



\- Python

\- Apache Kafka

\- Apache Spark Structured Streaming

\- PostgreSQL

\- Docker

\- Streamlit

\- SQL



\---



\## Project Structure



\---



\## Pipeline Flow



1\. Python producer generates simulated e-commerce orders.

2\. Orders are published to Kafka topics.

3\. Spark Structured Streaming consumes Kafka events.

4\. Processed data is stored in PostgreSQL.

5\. Streamlit dashboard displays real-time analytics.



\---



\## Features



\- Real-time order generation

\- Kafka message streaming

\- Spark stream processing

\- PostgreSQL storage

\- Revenue analytics

\- Order monitoring dashboard



\---



\## Running the Project



\### Start Docker services



```bash

docker-compose up -d# Real-Time E-Commerce Analytics Pipeline



A real-time data engineering project using:



\- Python

\- Apache Kafka

\- Apache Spark Structured Streaming

\- PostgreSQL

\- Streamlit

\- Docker





\## Architecture



Python Producer → Kafka → Spark Streaming → PostgreSQL → Streamlit Dashboard





\## Dashboard Preview



!\[Dashboard](images/e-commerce3.png)

