import os
import argparse
from datetime import datetime

def map_directory(directory):
    """
    Maps the content of the given directory and its subdirectories.
    Returns a list of strings representing the directory structure.
    """
    content_map = []
    for root, dirs, files in os.walk(directory):
        level = root.replace(directory, '').count(os.sep)
        indent = ' ' * 4 * level
        content_map.append(f'{indent}{os.path.basename(root)}/')
        sub_indent = ' ' * 4 * (level + 1)
        for file in files:
            content_map.append(f'{sub_indent}{file}')
    return content_map

def save_map_to_file(content_map, output_file):
    """
    Saves the content map to a text file.
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"Directory Map created on {datetime.now()}\n\n")
        for item in content_map:
            f.write(f"{item}\n")

def main():
    parser = argparse.ArgumentParser(description="Map directory contents and save to a text file.")
    parser.add_argument("-d", "--directory", default=".", help="Directory to map (default: current directory)")
    parser.add_argument("-o", "--output", default="directory_map.txt", help="Output file name (default: directory_map.txt)")
    args = parser.parse_args()

    directory = os.path.abspath(args.directory)
    output_file = args.output

    print(f"Mapping directory: {directory}")
    content_map = map_directory(directory)
    save_map_to_file(content_map, output_file)
    print(f"Directory map saved to: {output_file}")

if __name__ == "__main__":
    main()