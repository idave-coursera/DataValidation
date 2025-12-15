import yaml

def process_view_paths(input_file="resources/viewPaths.txt"):
    with open(input_file, "r") as f:
        paths = [line.strip() for line in f if line.strip()]

    for path in paths:
        try:
            with open(path, "r") as f2:
                data = yaml.safe_load(f2)
                if data and 'models' in data and len(data['models']) > 0:
                    model = data['models'][0]
                    print(model.get('name'), model.get('description'))
        except FileNotFoundError:
            print(f"Warning: File not found: {path}")
        except Exception as e:
            print(f"Error processing {path}: {e}")

if __name__ == "__main__":
    process_view_paths()
