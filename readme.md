# Real-Time Comment Classification and Visualization

## Overview

This project provides a solution for real-time comment classification and visualization using Python. A Kafka producer (`producer.py`) to publish comments to a Kafka topic, and a Kafka consumer (`consumer.py`) to classify comments and display real-time visualizations.

## Features

- **Real-Time Comment Classification**: Comments are published to a Kafka topic and then classified as either "Positive" or "Negative" using a Random Forest model.

- **Dynamic Visualization**: The project uses Dash by Plotly to create a dynamic web-based dashboard that displays real-time statistics of positive and negative comments. You can also view the last 10 comments in real-time.

- **Dockerized Deployment**: The application is Dockerized, allowing you to easily deploy it using Docker Compose. Docker containers are provided for MySQL, Kafka, and the application itself, simplifying the setup and ensuring consistency across environments.

## Usage

1. **Running the Application**:

   - Use Docker Compose to start the entire application stack. Run the following command in the project directory:

     ```
     docker-compose up
     ```

2. **Stopping the Application**:

   - To stop the application and remove the containers, you can use the following command:

     ```
     docker-compose down
     ```

   - This will gracefully stop the producer and consumer processes, as well as the associated services.

## License

This project is licensed under the [MIT License](LICENSE).
