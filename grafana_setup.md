# University Assignment: Real-Time Traffic Analysis

## Appendix A: Grafana Visualization Setup Guide

### 1.0 Introduction

This guide provides a comprehensive walkthrough for configuring Grafana to visualize the real-time traffic metrics processed by our Kafka pipeline and stored in a PostgreSQL database. By following these steps, we will connect Grafana to the database and build a basic dashboard to monitor key performance indicators such as traffic volume and sensor availability. This setup is crucial for the final stage of our data analysis project, enabling intuitive, at-a-glance insights into the traffic data.

### 2.0 Adding the PostgreSQL Data Source

The first step is to make Grafana aware of our PostgreSQL database where the metrics are stored.

**Step 1: Log into Grafana**
- Navigate to `http://localhost:3000` in your web browser.
- Log in with the default credentials (username: `admin`, password: `admin`). You may be prompted to change the password.

**Step 2: Navigate to Data Sources**
- On the left-hand menu, hover over the **Configuration** (gear) icon.
- Click on **"Data Sources"**.

![Navigate to Data Sources](https://grafana.com/docs/grafana/latest/datasources/add-a-data-source/img/add-data-source-navigate.png)

**Step 3: Add a New PostgreSQL Data Source**
- Click the **"Add data source"** button.
- In the search box, type `PostgreSQL` and select it from the list.

**Step 4: Configure the Connection**
- Fill in the connection details as follows:
    - **Host**: `postgres:5432` (This is the service name defined in `docker-compose.yml`)
    - **Database**: `trafficdb`
    - **User**: `trafficuser`
    - **Password**: `trafficpass`
    - **SSL Mode**: `disable` (For local development, we do not need SSL)
- The configuration should look like this:

![Configure PostgreSQL Connection](https://i.imgur.com/your-image-here.png)  <!-- Placeholder for a more specific image if needed -->

**Step 5: Test and Save**
- Scroll to the bottom of the page and click the **"Save & test"** button.
- You should see a green checkmark with the message "Database connection OK". If not, double-check the configuration details and ensure your Docker containers are running.

### 3.0 Creating a Visualization Dashboard

With the data source configured, we can now create a dashboard to display our metrics.

**Step 1: Create a New Dashboard**
- On the left-hand menu, click the **Dashboard** (four squares) icon.
- Click the **"New"** button in the top right corner, then select **"New Dashboard"**.

**Step 2: Add a New Panel**
- Click the **"+ Add visualization"** button.
- In the panel editor, ensure your PostgreSQL data source is selected in the dropdown menu.

**Step 3: Configure a Panel (Example: Hourly Average Volume)**
- Switch to the **"Code"** query editor.
- Enter the following SQL query to visualize the average volume over time:
  ```sql
  SELECT
    report_hour AS "time",
    avg_volume
  FROM hourly_avg_volume
  WHERE
    $__timeFilter(report_hour)
  ORDER BY 1
  ```
- On the right-hand side, you can set the panel title (e.g., "Hourly Average Vehicle Volume") and choose a visualization type, such as **"Time series"**.

**Step 4: Save the Panel and Dashboard**
- Click **"Apply"** in the top right to save the panel.
- Click the **Save** (floppy disk) icon at the top of the dashboard.
- Give your dashboard a name (e.g., "Real-Time Traffic Metrics") and click **"Save"**.

### 4.0 Conclusion

By completing this guide, we have successfully connected Grafana to our PostgreSQL database and created a foundational dashboard for visualizing real-time traffic metrics. This setup allows for the effective monitoring and analysis of the data being processed by our Kafka pipeline. Additional panels can be created for other metrics, such as `daily_peak_volume` and `daily_sensor_availability`, using similar SQL queries to build a comprehensive monitoring solution.
