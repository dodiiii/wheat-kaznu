import os
import json
import glob

DASHBOARDS_DIR = "/home/didar/uni/wheat-field-iot/dashboards"

def clean_overview_layout(data):
    # We will rebuild the panels list for 01-farm-overview.json
    new_panels = []
    
    # 3 Stat panels replacing the table
    stat_risk = {
        "id": 14,
        "type": "stat",
        "title": "🚦 Уровень угрозы",
        "gridPos": {
            "h": 6,
            "w": 4,
            "x": 0,
            "y": 11
        },
        "datasource": {
            "type": "influxdb",
            "uid": "influxdb"
        },
        "fieldConfig": {
            "defaults": {
                "color": {
                    "mode": "thresholds"
                },
                "mappings": [
                    {
                        "type": "value",
                        "options": {
                            "info": {
                                "text": "🟢 НОРМА",
                                "color": "green",
                                "index": 0
                            },
                            "warning": {
                                "text": "🟡 ВНИМАНИЕ",
                                "color": "orange",
                                "index": 1
                            },
                            "critical": {
                                "text": "🔴 ОПАСНОСТЬ",
                                "color": "red",
                                "index": 2
                            }
                        }
                    }
                ]
            }
        },
        "options": {
            "reduceOptions": {
                "calcs": [
                    "lastNotNull"
                ],
                "fields": "",
                "values": False
            },
            "textMode": "value",
            "colorMode": "background",
            "graphMode": "none"
        },
        "targets": [
            {
                "refId": "A",
                "datasource": {
                    "type": "influxdb",
                    "uid": "influxdb"
                },
                "query": "from(bucket: \"wheat_monitoring\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"agronomy_insights\")\n  |> filter(fn: (r) => r.field_id == \"${field_id}\")\n  |> filter(fn: (r) => r._field == \"risk_level\")\n  |> last()"
            }
        ]
    }

    stat_recommendation = {
        "id": 15,
        "type": "stat",
        "title": "📋 Рекомендуемое действие",
        "gridPos": {
            "h": 6,
            "w": 10,
            "x": 4,
            "y": 11
        },
        "datasource": {
            "type": "influxdb",
            "uid": "influxdb"
        },
        "fieldConfig": {
            "defaults": {
                "color": {
                    "mode": "fixed",
                    "fixedColor": "semi-flat-blue"
                }
            }
        },
        "options": {
            "reduceOptions": {
                "calcs": [
                    "lastNotNull"
                ]
            },
            "textMode": "value",
            "colorMode": "value",
            "graphMode": "none"
        },
        "targets": [
            {
                "refId": "A",
                "datasource": {
                    "type": "influxdb",
                    "uid": "influxdb"
                },
                "query": "from(bucket: \"wheat_monitoring\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"agronomy_insights\")\n  |> filter(fn: (r) => r.field_id == \"${field_id}\")\n  |> filter(fn: (r) => r._field == \"recommendation\")\n  |> last()"
            }
        ]
    }

    stat_insight = {
        "id": 16,
        "type": "stat",
        "title": "💡 Агрономическое обоснование",
        "gridPos": {
            "h": 6,
            "w": 10,
            "x": 14,
            "y": 11
        },
        "datasource": {
            "type": "influxdb",
            "uid": "influxdb"
        },
        "fieldConfig": {
            "defaults": {
                "color": {
                    "mode": "fixed",
                    "fixedColor": "text"
                }
            }
        },
        "options": {
            "reduceOptions": {
                "calcs": [
                    "lastNotNull"
                ]
            },
            "textMode": "value",
            "colorMode": "value",
            "graphMode": "none"
        },
        "targets": [
            {
                "refId": "A",
                "datasource": {
                    "type": "influxdb",
                    "uid": "influxdb"
                },
                "query": "from(bucket: \"wheat_monitoring\")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r._measurement == \"agronomy_insights\")\n  |> filter(fn: (r) => r.field_id == \"${field_id}\")\n  |> filter(fn: (r) => r._field == \"insight\")\n  |> last()"
            }
        ]
    }

    for p in data.get("panels", []):
        id_val = p.get("id")
        
        # 1. Header panel
        if id_val == 1:
            p["gridPos"] = {"h": 3, "w": 24, "x": 0, "y": 0}
            new_panels.append(p)
        
        # 2. Geomap panel
        elif id_val == 2:
            p["gridPos"] = {"h": 8, "w": 12, "x": 0, "y": 3}
            new_panels.append(p)
            
        # 3. Small KPI panels
        elif id_val == 3: # Avg Temp
            p["gridPos"] = {"h": 4, "w": 4, "x": 12, "y": 3}
            new_panels.append(p)
        elif id_val == 4: # Precip
            p["gridPos"] = {"h": 4, "w": 4, "x": 16, "y": 3}
            new_panels.append(p)
        elif id_val == 5: # Yield forecast
            p["gridPos"] = {"h": 4, "w": 4, "x": 20, "y": 3}
            new_panels.append(p)
        elif id_val == 6: # NDVI
            p["gridPos"] = {"h": 4, "w": 4, "x": 12, "y": 7}
            new_panels.append(p)
        elif id_val == 7: # Moisture
            p["gridPos"] = {"h": 4, "w": 4, "x": 16, "y": 7}
            new_panels.append(p)
        elif id_val == 8: # GDD
            p["gridPos"] = {"h": 4, "w": 4, "x": 20, "y": 7}
            new_panels.append(p)

        # 4. Table panel to replace
        elif id_val == 14:
            # We replace this with our three stat panels
            new_panels.append(stat_risk)
            new_panels.append(stat_recommendation)
            new_panels.append(stat_insight)
            
        # 5. Trend Chart Panels (move them to start at y: 17)
        elif id_val == 9: # Temp trend
            p["gridPos"] = {"h": 8, "w": 12, "x": 0, "y": 17}
            new_panels.append(p)
        elif id_val == 10: # Precip trend
            p["gridPos"] = {"h": 8, "w": 12, "x": 12, "y": 17}
            new_panels.append(p)
        elif id_val == 11: # NDVI trend
            p["gridPos"] = {"h": 8, "w": 12, "x": 0, "y": 25}
            new_panels.append(p)
        elif id_val == 12: # Moisture trend
            p["gridPos"] = {"h": 8, "w": 12, "x": 12, "y": 25}
            new_panels.append(p)
        elif id_val == 13: # GDD trend
            p["gridPos"] = {"h": 8, "w": 24, "x": 0, "y": 33}
            new_panels.append(p)
        else:
            # Any other panels just in case, shift down
            if "gridPos" in p:
                if p["gridPos"].get("y", 0) >= 2:
                    p["gridPos"]["y"] += 1
            new_panels.append(p)

    data["panels"] = new_panels
    return data

def adjust_sub_dashboards(data):
    for p in data.get("panels", []):
        id_val = p.get("id")
        if id_val == 1:
            # Make header panel larger
            p["gridPos"]["h"] = 3
        else:
            # All panels below header are shifted down by 1 unit
            if "gridPos" in p:
                if p["gridPos"].get("y", 0) >= 2:
                    p["gridPos"]["y"] += 1
    return data

def main():
    # 1. Update 01-farm-overview.json
    overview_path = os.path.join(DASHBOARDS_DIR, "01-farm-overview.json")
    print(f"Optimizing: {os.path.basename(overview_path)}")
    with open(overview_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data = clean_overview_layout(data)
    with open(overview_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print("Successfully optimized overview dashboard layout!")

    # 2. Update remaining dashboards
    other_files = ["02-meteo-soil.json", "03-growth-factors.json", "04-pests-diseases.json", "05-equipment.json"]
    for fname in other_files:
        fpath = os.path.join(DASHBOARDS_DIR, fname)
        print(f"Shifting header for: {fname}")
        with open(fpath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        data = adjust_sub_dashboards(data)
        with open(fpath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"Successfully adjusted header on {fname}!")

if __name__ == "__main__":
    main()
