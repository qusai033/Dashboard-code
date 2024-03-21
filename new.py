import paho.mqtt.client as mqtt
import psycopg2
import json
from datetime import datetime
import threading
import logging

updateInterval = 60  # 或者您希望的默认值

# SQL连接参数
db_params = {
    'host': '149.165.170.117',
    'port': 5432,
    'database': 'duodatabase',
    'user': 'exouser',
    'password': 'your_password',
}


# 连接到数据库
connection = psycopg2.connect(**db_params)

# 创建游标
cursor = connection.cursor()

# 创建表格
create_table_query_v2 = """
CREATE TABLE IF NOT EXISTS datatestv2 (
    id SERIAL PRIMARY KEY,
    value DOUBLE PRECISION,
    data_receive_time TIMESTAMP,
    study_id VARCHAR,
    sensor_id VARCHAR
);
"""
cursor.execute(create_table_query_v2)

# 提交更改
connection.commit()

# 初始化变量
temperatureData = {
    'sensor1': [],
    'sensor2': [],
    'sensor3': [],
    'sensor4': []
}
barrierResistanceData = []
elapsedSeconds = 0

# Function to format time labels for the graph
def formatTimeLabel():
    if elapsedSeconds < 60:
        return f"{elapsedSeconds}s"
    elif elapsedSeconds < 3600:
        return f"{int(elapsedSeconds / 60)}m"
    else:
        return f"{int(elapsedSeconds / 3600)}h"

# Function to update data and adjust interval
# 更新数据并调整间隔的函数
def updateData():
    global elapsedSeconds, updateInterval

    formattedTime = formatTimeLabel()
    updateGraphs()
    updateTable()

    # 更新间隔
    elapsedSeconds += 1
    if elapsedSeconds >= 60 and elapsedSeconds < 3600:
        updateInterval = 60
    elif elapsedSeconds >= 3600:
        updateInterval = 3600

    # 使用更新后的间隔递归调用函数
    threading.Timer(updateInterval, updateData).start()



# Function to update graphs with new data
def updateGraphs():
    # Update graphs here
    pass

# Function to update the table with the latest 10 data points
def updateTable():
    # Update table here
    pass

# Function to handle incoming MQTT messages
def on_message(client, userdata, msg):
    global elapsedSeconds

    try:
        data = json.loads(msg.payload)
        print("Received JSON data:", data)

        if msg.topic == 'sensor_data/temperature':
            temperatureData['sensor1'].append(data['sensor1'])
            temperatureData['sensor2'].append(data['sensor2'])
            temperatureData['sensor3'].append(data['sensor3'])
            temperatureData['sensor4'].append(data['sensor4'])
        elif msg.topic == 'evom_data/teer':
            if len(barrierResistanceData) == 64:
                temperatureData['sensor1'].pop(0)
            barrierResistanceData.append(data['barrierResistance'])
                # 将数据插入新表格
        insert_query_v2 = """
        INSERT INTO datatestv2 (value, data_receive_time, study_id, sensor_id)
        VALUES (%s, %s, %s, %s);
        """
        cursor.execute(insert_query_v2, (
            data.get('barrierResistance', None),  # 使用barrierResistance的值，如果不存在则为None
            datetime.now(),
            'your_study_id',  # 替换为您的study_id
            'your_sensor_id'  # 替换为您的sensor_id
        ))
        # Commit transaction after successfully inserting data
        connection.commit()

        # Update other data and charts
        updateData()

    except Exception as e:
        error_message = f"Error parsing JSON: {e}\n"
        print(error_message)
        print("Received JSON data:", msg.payload)

        # Add log entry
        logging.error(error_message)

        # Write error data to a file for analysis
        with open("error_data.json", "a") as f:
            f.write(error_message)
            f.write(f"Received JSON data: {msg.payload}\n\n")

        # Rollback transaction
        connection.rollback()

# Initialize MQTT client
client = mqtt.Client()
client.on_message = on_message

# Connect to MQTT broker
client.connect("broker.hivemq.com", 1883, 60)

# Subscribe to topics
client.subscribe("sensor_data/temperature")
client.subscribe("evom_data/teer")

# Start the MQTT loop to maintain connection
client.loop_forever()

# Close connection
cursor.close()
connection.close()


