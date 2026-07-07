import os
import json
import glob

DASHBOARDS_DIR = "/home/didar/uni/wheat-field-iot/dashboards"

field_variable = {
    "current": {
        "selected": True,
        "text": "All",
        "value": [
            "$__all"
        ]
    },
    "datasource": {
        "type": "influxdb",
        "uid": "influxdb"
    },
    "definition": "",
    "hide": 0,
    "includeAll": True,
    "label": "Поле",
    "multi": True,
    "name": "field_id",
    "options": [],
    "query": "from(bucket: \"wheat_monitoring\")\n  |> range(start: -3y)\n  |> filter(fn: (r) => r._measurement == \"weather\")\n  |> group(columns: [\"field_id\", \"field_name\"])\n  |> limit(n: 1)\n  |> group()\n  |> rename(columns: {field_name: \"__text\", field_id: \"__value\"})\n  |> keep(columns: [\"__text\", \"__value\"])",
    "refresh": 1,
    "regex": "",
    "skipUrlSync": False,
    "sort": 1,
    "type": "query",
    "allValue": ".*"
}

def update_dashboard(filepath):
    print(f"Processing: {os.path.basename(filepath)}")
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Part 1: Ensure/Update variables
    data = json.loads(content)
    
    # Check if templating section exists
    if "templating" not in data or not isinstance(data["templating"], dict):
        data["templating"] = {"list": []}
    
    var_list = data["templating"].get("list", [])
    found = False
    for i, var in enumerate(var_list):
        if var.get("name") == "field_id":
            # Update existing variable to support multi-select/All
            var_list[i] = field_variable
            found = True
            break
    
    if not found:
        var_list.append(field_variable)
    
    data["templating"]["list"] = var_list

    # Reserialize to modify strings/queries
    new_content = json.dumps(data, indent=4, ensure_ascii=False)

    # Part 2: Replace standard field_id filtering with regex matching
    # Support escaped and unescaped versions
    new_content = new_content.replace('r.field_id == \\"${field_id}\\"', 'r.field_id =~ /^${field_id:regex}$/')
    new_content = new_content.replace('r.field_id == "${field_id}"', 'r.field_id =~ /^${field_id:regex}$/')
    new_content = new_content.replace('r.field_id == \\\"${field_id}\\\"', 'r.field_id =~ /^${field_id:regex}$/')

    # Part 3: For overview dashboard, add filter after measurement
    if os.path.basename(filepath) == "01-farm-overview.json":
        measurements = ["weather", "growth", "soil_dynamic", "pest_disease", "agronomy_insights", "equipment"]
        for m in measurements:
            # We want to replace e.g.:
            # |> filter(fn: (r) => r._measurement == "weather")
            # or with escaped quotes
            target = f'|> filter(fn: (r) => r._measurement == "{m}")'
            replacement = f'|> filter(fn: (r) => r._measurement == "{m}")\\n  |> filter(fn: (r) => r.field_id =~ /^${{field_id:regex}}$/)'
            new_content = new_content.replace(target, replacement)

            target_escaped = f'|> filter(fn: (r) => r._measurement == \\"{m}\\")'
            replacement_escaped = f'|> filter(fn: (r) => r._measurement == \\"{m}\\\")\\n  |> filter(fn: (r) => r.field_id =~ /^${{field_id:regex}}$/)'
            new_content = new_content.replace(target_escaped, replacement_escaped)

            target_escaped3 = f'|> filter(fn: (r) => r._measurement == \\\\"{m}\\\\")'
            replacement_escaped3 = f'|> filter(fn: (r) => r._measurement == \\\\"{m}\\\\")\\n  |> filter(fn: (r) => r.field_id =~ /^${{field_id:regex}}$/)'
            new_content = new_content.replace(target_escaped3, replacement_escaped3)

    return new_content

def main():
    files = glob.glob(os.path.join(DASHBOARDS_DIR, "*.json"))
    for filepath in files:
        new_json_str = update_dashboard(filepath)
        # Validate JSON format before writing
        try:
            json.loads(new_json_str)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_json_str)
            print(f"Successfully updated and validated: {os.path.basename(filepath)}")
        except Exception as e:
            print(f"Error parsing json for {os.path.basename(filepath)}: {e}")

if __name__ == "__main__":
    main()
