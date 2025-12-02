import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, render_template, jsonify

AIO_USERNAME = os.getenv("AIO_USERNAME", "DMirijello")
AIO_KEY = os.getenv("AIO_KEY")  
AIO_BASE = f"https://io.adafruit.com/api/v2/{AIO_USERNAME}/feeds"

DB_CONN = os.getenv("DB_CONN") 

def get_db():
    return psycopg2.connect(DB_CONN, cursor_factory=RealDictCursor)

def aio_send(feed, value):
    url = f"{AIO_BASE}/{feed}/data"
    headers = {"X-AIO-Key": AIO_KEY, "Content-Type": "application/json"}
    return requests.post(url, headers=headers, json={"value": value})

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("home.html", title="Home")

@app.route("/about")
def about():
    return render_template("about.html", title="About")

@app.route("/environment")
def environment():
    return render_template("environment.html", title="Environmental Data")

@app.route("/security")
def security():
    return render_template("security.html", title="Security")

@app.route("/devices")
def devices():
    return render_template("devices.html", title="Device Control")

@app.route("/api/security/set/<state>", methods=["POST"])
def set_security(state):
    try:
        url = f"{AIO_BASE}/security-system/data"
        headers = {"X-AIO-Key": AIO_KEY, "Content-Type": "application/json"}

        value = "ON" if state.lower() == "on" else "OFF"
        requests.post(url, headers=headers, json={"value": value})

        return jsonify({"status": "ok", "security": value})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/security/status")
def security_status():
    try:
        url = f"{AIO_BASE}/security-system/data?limit=1"
        headers = {"X-AIO-Key": AIO_KEY}
        r = requests.get(url, headers=headers)

        if r.status_code != 200:
            return jsonify({"status": "unknown"})

        data = r.json()
        return jsonify({"status": data[0]["value"]})
    except:
        return jsonify({"status": "unknown"})

@app.route("/api/intrusions/<date>")
def intrusions(date):
    try:
        conn = get_db()
        cur = conn.cursor()

        query = """
            SELECT timestamp
            FROM sensor_data
            WHERE sensor = 'motion'
            AND value = 1
            AND DATE(timestamp) = %s
            ORDER BY timestamp ASC;
        """

        cur.execute(query, (date,))
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/history/<sensor>/<date>")
def history(sensor, date):
    try:
        conn = get_db()
        cur = conn.cursor()

        query = """
            SELECT timestamp, value
            FROM sensor_data
            WHERE sensor = %s
            AND DATE(timestamp) = %s
            ORDER BY timestamp ASC;
        """

        cur.execute(query, (sensor, date))
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/live/<feed>")
def live_data(feed):
    try:
        url = f"{AIO_BASE}/{feed}/data?limit=20"
        headers = {"X-AIO-Key": AIO_KEY}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            return jsonify({"error": "Failed to fetch"}), 500

        return jsonify(response.json())

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/api/device/<feed>/<value>", methods=["POST"])
def device_command(feed, value):
    try:
        aio_send(feed, value)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
