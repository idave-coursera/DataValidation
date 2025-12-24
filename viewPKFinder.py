import json

import yaml

from config import BASE_PATH


def load_view_keys():
    input_file = "resources/viewPaths.txt"
    found_count = 0
    not_found_tables = []
    key_data = {}

    with open(input_file, "r") as file:
        paths = [BASE_PATH+line.strip() for line in file if line.strip()]
    for path in paths:
        with open(path, "r") as f2:
            yobj = yaml.safe_load(f2)
            table = path.split("/")[-1].split(".")[0]
            if table == "sources" :
                continue
            description = yobj.get("models",[{}])[0].get("description","")
            column_data = yobj.get("models",[{}])[0].get("columns")
            start_index = description.rfind("Primary Key")
            
            if start_index == -1 or column_data is None:
                not_found_tables.append(table)
                continue
            columns = [x['name'] for x in column_data]

            if len(columns) == 0:
                not_found_tables.append(table)
                continue
            base_markers = ["Change Log", "Changelog", "Not null", "MEGA Equivalent Table", "Foreign Key","Not Null","Data grain"]
            search_markers = set()
            for m in base_markers:
                # Add variations: original, lower, upper, no-space, snake-case
                variations = [m, m.replace(" ", ""), m.replace(" ", "_")]
                for v in variations:
                    search_markers.add(v)
                    search_markers.add(v.lower())
                    search_markers.add(v.upper())

            possible_indices = []
            for marker in search_markers:
                idx = description.find(marker, start_index + 1) # Look after the start of Primary Key
                if idx != -1:
                    possible_indices.append(idx)
            
            # Use the smallest valid index (nearest section), or default to end of string
            end_index = min(possible_indices) if possible_indices else len(description)
            
            primary_keys_string = description[start_index:end_index]
            primary_keys = [x for x in columns if (x in primary_keys_string)]

            if len(primary_keys) == 0:
                not_found_tables.append(table)
                continue

            found_count += 1
            key_data[table] = primary_keys

    print(f"Found: {found_count}")
    print(f"Not Found: {len(not_found_tables)}")
    json.dump(key_data, open('resources/view_keys.json', "w"),indent=4)


if __name__ == "__main__":
    load_view_keys()
