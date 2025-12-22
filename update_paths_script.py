import os
from config import BASE_PATH

FILES = ["resources/viewPaths.txt", "resources/ingestionPath.txt"]

def update_file(filepath):
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return

    with open(filepath, 'r') as f:
        lines = f.readlines()
    
    new_lines = []
    modified_count = 0
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("/"): # Check if not already absolute
            # Avoid double prefixing if run multiple times
            if not stripped.startswith(BASE_PATH):
                new_lines.append(os.path.join(BASE_PATH, stripped) + "\n")
                modified_count += 1
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)
            
    with open(filepath, 'w') as f:
        f.writelines(new_lines)
    print(f"Updated {filepath} with {modified_count} lines modified.")

if __name__ == "__main__":
    for f in FILES:
        update_file(f)

