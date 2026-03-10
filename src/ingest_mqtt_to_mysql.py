import os
import json
import time
import csv
import io
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import mysql.connector


DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "secret")
DB_NAME = os.getenv("DB_NAME", "BalerDB")

MQTT_HOST = os.getenv("MQTT_HOST", "mqtt")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "#")


FIELDS = [
    "bale_number",
    "recipe_number",
    "material_name",
    "user_id",
    "username",
    "customer_number",
    "shift_number",
    "timestamp",
    "kwh_used",
    "bale_length",
    "wires_vertical",
    "wires_horizontal",
    "knots_vertical",
    "knots_horizontal",
    "weight",
    "volume",
    "oil_temperature",
    "oil_level",
    "total_time",
    "auto_time",
    "standby_time",
    "empty_time",
    "valve_lp",
    "valve_hp",
    "valve_ko1",
    "valve_ko2",
    "valve_kd1",
    "valve_kd2",
    "valve_rp1",
    "valve_rp2",
    "valve_rr1",
    "valve_rr2",
    "valve_ch",
    "valve_mes",
]


def db_connect():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
    )


def to_dt(value):
    if value is None:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    if isinstance(value, dict):
        try:
            year = int(value.get("year", 2000))
            month = int(value.get("month", 1))
            day = int(value.get("day", 1))
            hour = int(value.get("hour", 0))
            minute = int(value.get("minute", 0))
            second = int(value.get("second", 0))
            return datetime(year, month, day, hour, minute, second)
        except Exception:
            return datetime.now(timezone.utc).replace(tzinfo=None)

    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc).replace(tzinfo=None)
        return datetime.fromtimestamp(value, tz=timezone.utc).replace(tzinfo=None)

    s = str(value).strip()
    if s.isdigit():
        return to_dt(int(s))

    try:
        s2 = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s2).astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return datetime.now(timezone.utc).replace(tzinfo=None)


def safe_int(v):
    if v is None:
        return None
    try:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if s == "":
            return None
        return int(float(s.replace(",", ".")))
    except Exception:
        return None


def safe_float(v):
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if s == "":
            return None
        return float(s.replace(",", "."))
    except Exception:
        return None


def extract_payload_dict(payload_text):
    # 1) JSON dict
    try:
        obj = json.loads(payload_text)
        if isinstance(obj, dict):
            # allow nesting: {"data": {...}}
            if "data" in obj and isinstance(obj["data"], dict):
                return obj["data"]
            return obj
    except Exception:
        pass

    # 2) CSV (either header+row, or single row where headers match known)
    # We support:
    # - first line header, second line data
    # - or single line with delimiter and expected headers in first line
    try:
        text = payload_text.strip()
        if not text:
            return None

        f = io.StringIO(text)
        # sniff delimiter
        sample = text[:2000]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            delim = dialect.delimiter
        except Exception:
            delim = ","

        reader = list(csv.reader(f, delimiter=delim))
        if not reader:
            return None

        if len(reader) >= 2:
            header = [h.strip() for h in reader[0]]
            row = reader[1]
            if any(h in FIELDS for h in header):
                d = {}
                for i, h in enumerate(header):
                    if i < len(row):
                        d[h] = row[i]
                return d

        # single line "key=value" not supported; keep raw only
        return None
    except Exception:
        return None


def insert_raw(cur, topic, payload_obj, payload_text):
    cur.execute(
        "INSERT INTO mqtt_raw (topic, payload, payload_text) VALUES (%s, %s, %s)",
        (topic, json.dumps(payload_obj) if payload_obj is not None else None, payload_text),
    )
    cur.execute("SELECT LAST_INSERT_ID()")
    return cur.fetchone()[0]


def insert_bale_cycle(cur, raw_id, p):
    # map fields
    ts = to_dt(p.get("timestamp"))

    # strings
    material_name = p.get("material_name")
    username = p.get("username")

    # ints
    bale_number = safe_int(p.get("bale_number"))
    recipe_number = safe_int(p.get("recipe_number"))
    user_id = safe_int(p.get("user_id"))
    customer_number = safe_int(p.get("customer_number"))
    shift_number = safe_int(p.get("shift_number"))

    bale_length = safe_int(p.get("bale_length"))
    wires_vertical = safe_int(p.get("wires_vertical"))
    wires_horizontal = safe_int(p.get("wires_horizontal"))
    knots_vertical = safe_int(p.get("knots_vertical"))
    knots_horizontal = safe_int(p.get("knots_horizontal"))
    weight = safe_int(p.get("weight"))
    volume = safe_int(p.get("volume"))
    oil_temperature = safe_int(p.get("oil_temperature"))
    oil_level = safe_int(p.get("oil_level"))

    total_time = safe_int(p.get("total_time"))
    auto_time = safe_int(p.get("auto_time"))
    standby_time = safe_int(p.get("standby_time"))
    empty_time = safe_int(p.get("empty_time"))

    valve_lp = safe_int(p.get("valve_lp"))
    valve_hp = safe_int(p.get("valve_hp"))
    valve_ko1 = safe_int(p.get("valve_ko1"))
    valve_ko2 = safe_int(p.get("valve_ko2"))
    valve_kd1 = safe_int(p.get("valve_kd1"))
    valve_kd2 = safe_int(p.get("valve_kd2"))
    valve_rp1 = safe_int(p.get("valve_rp1"))
    valve_rp2 = safe_int(p.get("valve_rp2"))
    valve_rr1 = safe_int(p.get("valve_rr1"))
    valve_rr2 = safe_int(p.get("valve_rr2"))
    valve_ch = safe_int(p.get("valve_ch"))
    valve_mes = safe_int(p.get("valve_mes"))

    # floats
    kwh_used = safe_float(p.get("kwh_used"))

    cur.execute(
        """
        INSERT INTO bale_cycle (
          ts,
          bale_number, recipe_number, material_name,
          user_id, username, customer_number, shift_number,
          kwh_used, bale_length, wires_vertical, wires_horizontal,
          knots_vertical, knots_horizontal, weight, volume,
          oil_temperature, oil_level,
          total_time, auto_time, standby_time, empty_time,
          valve_lp, valve_hp, valve_ko1, valve_ko2, valve_kd1, valve_kd2,
          valve_rp1, valve_rp2, valve_rr1, valve_rr2, valve_ch, valve_mes,
          raw_id
        ) VALUES (
          %s,
          %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s, %s,
          %s, %s,
          %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s, %s,
          %s
        )
        ON DUPLICATE KEY UPDATE
          recipe_number=VALUES(recipe_number),
          material_name=VALUES(material_name),
          user_id=VALUES(user_id),
          username=VALUES(username),
          customer_number=VALUES(customer_number),
          shift_number=VALUES(shift_number),
          kwh_used=VALUES(kwh_used),
          bale_length=VALUES(bale_length),
          wires_vertical=VALUES(wires_vertical),
          wires_horizontal=VALUES(wires_horizontal),
          knots_vertical=VALUES(knots_vertical),
          knots_horizontal=VALUES(knots_horizontal),
          weight=VALUES(weight),
          volume=VALUES(volume),
          oil_temperature=VALUES(oil_temperature),
          oil_level=VALUES(oil_level),
          total_time=VALUES(total_time),
          auto_time=VALUES(auto_time),
          standby_time=VALUES(standby_time),
          empty_time=VALUES(empty_time),
          valve_lp=VALUES(valve_lp),
          valve_hp=VALUES(valve_hp),
          valve_ko1=VALUES(valve_ko1),
          valve_ko2=VALUES(valve_ko2),
          valve_kd1=VALUES(valve_kd1),
          valve_kd2=VALUES(valve_kd2),
          valve_rp1=VALUES(valve_rp1),
          valve_rp2=VALUES(valve_rp2),
          valve_rr1=VALUES(valve_rr1),
          valve_rr2=VALUES(valve_rr2),
          valve_ch=VALUES(valve_ch),
          valve_mes=VALUES(valve_mes),
          raw_id=VALUES(raw_id)
        """,
        (
            ts,
            bale_number, recipe_number, material_name,
            user_id, username, customer_number, shift_number,
            kwh_used, bale_length, wires_vertical, wires_horizontal,
            knots_vertical, knots_horizontal, weight, volume,
            oil_temperature, oil_level,
            total_time, auto_time, standby_time, empty_time,
            valve_lp, valve_hp, valve_ko1, valve_ko2, valve_kd1, valve_kd2,
            valve_rp1, valve_rp2, valve_rr1, valve_rr2, valve_ch, valve_mes,
            raw_id,
        ),
    )


db = None
cur = None


def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[mqtt] connected rc={rc} subscribe={MQTT_TOPIC}")
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    global cur
    try:
        payload_text = msg.payload.decode("utf-8", errors="replace")
    except Exception:
        payload_text = str(msg.payload)

    payload_dict = extract_payload_dict(payload_text)

    try:
        raw_id = insert_raw(
            cur,
            msg.topic,
            payload_dict if isinstance(payload_dict, dict) else None,
            payload_text
        )

        if (
            isinstance(payload_dict, dict)
            and isinstance(payload_dict.get("fields"), dict)
        ):
            parsed = payload_dict["fields"]
            insert_bale_cycle(cur, raw_id, parsed)
            print(f"[db] inserted bale_cycle bale_number={parsed.get('bale_number')}")
        else:
            print(f"[db] raw only topic={msg.topic}")

    except Exception as e:
        print(f"[db] write error: {e}")


def main():
    global db, cur

    # Wait for DB
    while True:
        try:
            db = db_connect()
            cur = db.cursor()
            print("[db] connected")
            break
        except Exception as e:
            print(f"[db] waiting... {e}")
            time.sleep(2)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            print(f"[mqtt] connecting {MQTT_HOST}:{MQTT_PORT}")
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            print(f"[mqtt] reconnecting... {e}")
            time.sleep(2)


if __name__ == "__main__":
    main()