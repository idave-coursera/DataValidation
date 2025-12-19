import yaml

from config import BASE_PATH


def load_view_keys():
    input_file = "resources/viewPaths.txt"
    ans = []
    with open(input_file, "r") as file:
        paths = [BASE_PATH+line.strip() for line in file if line.strip()]
    for path in paths:
        with open(path, "r") as f2:
            yobj = yaml.safe_load(f2)
            table = path.split("/")[-1].split(".")[0]
            description = yobj.get("models",[{}])[0].get("description","")
            primary_keys = description

            print(table, primary_keys)
            ans.append(primary_keys)



if __name__ == "__main__":
    load_view_keys()
