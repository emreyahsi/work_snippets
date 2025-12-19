import os

def list_target_files(base_folder, subfolders, extensions):
    """
    List all files with specific extensions in given subfolders
    """
    file_paths = []
    for subfolder in subfolders:
        folder_path = os.path.join(base_folder, subfolder)
        if not os.path.exists(folder_path):
            continue
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if any(file.endswith(ext) for ext in extensions):
                    file_paths.append(os.path.join(root, file))
    return file_paths

def search_filename_in_flow_files(filename, base_folder):
    """
    Search if filename exists in any .flow or .subflow file
    """
    for root, dirs, files in os.walk(base_folder):
        for file in files:
            if file.endswith('.flow') or file.endswith('.subflow'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        contents = f.read()
                        if filename in contents:
                            return True
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
    return False

def main(base_folder):
    # List all .sql and .json files in conf and sql subfolders
    target_files = list_target_files(base_folder, ['conf', 'sql'], ['.sql', '.json'])
    
    unused_files = []
    
    for file_path in target_files:
        filename = os.path.basename(file_path)
        if not search_filename_in_flow_files(filename, base_folder):
            unused_files.append(file_path)
    
    return unused_files

# Example usage
base_folders = ['etl-main','migration','test']  # Add multiple folders here as needed
move_flag = 1  # Set to 1 to move files, 0 to only report

print("Searching for unused files...")

all_unused_files = []
folder_stats = {}

# Process each base folder
for base_folder in base_folders:
    print(f"\nProcessing folder: {base_folder}")
    unused_files = main(base_folder)
    all_unused_files.extend(unused_files)
    
    sql_count = sum(1 for f in unused_files if f.endswith('.sql'))
    json_count = sum(1 for f in unused_files if f.endswith('.json'))
    folder_stats[base_folder] = {
        'sql': sql_count,
        'json': json_count,
        'total': len(unused_files),
        'files': unused_files
    }

# Count total by file type
total_sql_count = sum(1 for f in all_unused_files if f.endswith('.sql'))
total_json_count = sum(1 for f in all_unused_files if f.endswith('.json'))

moved_files = []

if move_flag == 1:
    import shutil
    
    # Move files for each base folder
    for base_folder in base_folders:
        unused_files = folder_stats[base_folder]['files']
        
        if not unused_files:
            continue
            
        # Create unused directories
        unused_base = os.path.join(base_folder, 'unused')
        unused_conf = os.path.join(unused_base, 'conf')
        unused_sql = os.path.join(unused_base, 'sql')
        
        os.makedirs(unused_conf, exist_ok=True)
        os.makedirs(unused_sql, exist_ok=True)
        
        # Move files to unused folders
        for file_path in unused_files:
            filename = os.path.basename(file_path)
            
            if file_path.endswith('.json'):
                dest_path = os.path.join(unused_conf, filename)
                shutil.move(file_path, dest_path)
                moved_files.append((file_path, dest_path))
                print(f"Moved: {file_path} -> {dest_path}")
            elif file_path.endswith('.sql'):
                dest_path = os.path.join(unused_sql, filename)
                shutil.move(file_path, dest_path)
                moved_files.append((file_path, dest_path))
                print(f"Moved: {file_path} -> {dest_path}")
else:
    # Just list the files without moving
    for file_path in all_unused_files:
        print(file_path)

# Write results to output file
output_file = 'unused_objects.txt'
with open(output_file, 'w', encoding='utf-8') as f:
    f.write("=" * 50 + "\n")
    f.write("UNUSED FILES REPORT\n")
    f.write("=" * 50 + "\n\n")
    
    # Write details for each folder
    for base_folder in base_folders:
        stats = folder_stats[base_folder]
        f.write(f"\nFolder: {base_folder}\n")
        f.write("-" * 50 + "\n")
        f.write(f"SQL files: {stats['sql']}, JSON files: {stats['json']}, Total: {stats['total']}\n\n")
        
        if move_flag == 1 and stats['files']:
            f.write("MOVED FILES:\n")
            for original, destination in moved_files:
                if original.startswith(base_folder):
                    f.write(f"  {original} -> {destination}\n")
        elif stats['files']:
            f.write("UNUSED FILES (not moved):\n")
            for file_path in stats['files']:
                f.write(f"  {file_path}\n")
        f.write("\n")
    
    f.write("=" * 50 + "\n")
    f.write("OVERALL SUMMARY\n")
    f.write("=" * 50 + "\n")
    f.write(f"Total folders processed: {len(base_folders)}\n")
    f.write(f"Total unused SQL files: {total_sql_count}\n")
    f.write(f"Total unused JSON files: {total_json_count}\n")
    f.write(f"Total unused files: {len(all_unused_files)}\n")
    if move_flag == 1:
        f.write("Files have been moved to respective 'unused' folders\n")
    else:
        f.write("Files were NOT moved (move_flag = 0)\n")
    f.write("=" * 50 + "\n")

print(f"\n{'=' * 50}")
print("OVERALL SUMMARY")
print('=' * 50)
print(f"Total folders processed: {len(base_folders)}")
for base_folder in base_folders:
    stats = folder_stats[base_folder]
    print(f"\n{base_folder}:")
    print(f"  SQL: {stats['sql']}, JSON: {stats['json']}, Total: {stats['total']}")
print(f"\nGrand Total:")
print(f"  SQL files: {total_sql_count}")
print(f"  JSON files: {total_json_count}")
print(f"  Total files: {len(all_unused_files)}")
if move_flag == 1:
    print("Files have been moved to respective 'unused' folders")
else:
    print("Files were NOT moved (move_flag = 0)")
print('=' * 50)
print(f"\nResults written to {output_file}")
