import os
import subprocess
from collections import defaultdict
import platform
import argparse

EXCLUDED_KEYS = {"snapshot name", "file name", "creation time"}

def print_os_specific_data():
    system_info = {
        "System": platform.system(),
        "Node Name": platform.node(),
        "Release": platform.release(),
        "Version": platform.version(),
        "Machine": platform.machine(),
        "Processor": platform.processor(),
        "Architecture": "x64" if platform.architecture()[0] == "64bit" else "x86"
    }

    max_key_length = max(len(key) for key in system_info.keys())

    print("System Information:")
    for key, value in system_info.items():
        print(f"{key:<{max_key_length}} : {value}")

def crawl_directory(path, level=0, max_level=3, max_entries=10000, attributes=defaultdict(set), mask_bit_count=0, default_acl_count=0, entry_count=0):
    if level > max_level or entry_count >= max_entries:
        return mask_bit_count, default_acl_count, entry_count
    
    try:
        # List all items in the current directory
        with os.scandir(path) as it:
            for entry in it:
                if entry_count >= max_entries:
                    break

                # Increment the entry counter
                entry_count += 1
                
                # Execute the mmlsattr -L [fileName] command for each entry
                try:
                    result = subprocess.run(["/usr/lpp/mmfs/bin/mmlsattr", "-L", entry.path], capture_output=True, text=True, check=True)
                    # Parse the output and collect key-value pairs
                    for line in result.stdout.splitlines():
                        if ': ' in line:
                            key, value = line.split(': ', 1)
                            attributes[key].add(value)
                except subprocess.CalledProcessError as e:
                    print(f"Error executing mmlsattr for {entry.path}: {e}")
                except FileNotFoundError:
                    print(f"Command /usr/lpp/mmfs/bin/mmlsattr not found for {entry.path}. Ensure it is installed and in your PATH.")
                    return mask_bit_count, default_acl_count, entry_count
                
                # Execute the mmgetacl -k posix [filename] command for each entry
                try:
                    acl_result = subprocess.run(["/usr/lpp/mmfs/bin/mmgetacl", "-k", "posix", entry.path], capture_output=True, text=True, check=True)
                    # Check if the output contains the mask bit
                    if "mask:" in acl_result.stdout:
                        mask_bit_count += 1
                    # Check if the output contains default ACLs
                    if "default:" in acl_result.stdout:
                        default_acl_count += 1
                except subprocess.CalledProcessError as e:
                    print(f"Error executing mmgetacl for {entry.path}: {e}")
                except FileNotFoundError:
                    print(f"Command /usr/lpp/mmfs/bin/mmgetacl not found for {entry.path}. Ensure it is installed and in your PATH.")
                    return mask_bit_count, default_acl_count, entry_count

                # If the entry is a directory, recursively crawl the next level
                if entry.is_dir():
                    mask_bit_count, default_acl_count, entry_count = crawl_directory(
                        entry.path, level + 1, max_level, max_entries, attributes, mask_bit_count, default_acl_count, entry_count
                    )
                    if entry_count >= max_entries:
                        break
    except PermissionError:
        print("Permission denied for", path)
    except FileNotFoundError:
        print("File not found", path)
    except Exception as e:
        print(f"An error occurred while accessing {path}: {e}")
    
    return mask_bit_count, default_acl_count, entry_count

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl directories and collect attribute information.")
    parser.add_argument("root_directory", type=str, help="The root directory to crawl")
    parser.add_argument("--print_values", action="store_true", help="Print all values for each key instead of just the distinct count")
    args = parser.parse_args()

    print_os_specific_data()
    
    root_directory = args.root_directory
    attributes = defaultdict(set)
    mask_bit_count, default_acl_count, entry_count = crawl_directory(root_directory, attributes=attributes)
    
    # Calculate the maximum length of the keys for alignment
    max_key_length = max(len(key) for key in attributes.keys() if key not in EXCLUDED_KEYS)
    
    # Print the count of distinct values or all values for each key, aligned, excluding specific keys
    print("\nDistinct counts or values for each key:")
    for key, values in attributes.items():
        if key not in EXCLUDED_KEYS:
            print(f"{key:<{max_key_length}} :", end=" ")
            if args.print_values:
                print("|".join(sorted(values)))  # Print all values for the key
            else:
                print(f"{len(values):<10} distinct values")  # Print the count of distinct values
        
    # Print the count of entries with the mask bit set and default ACLs set
    print(f"\n{'Entries with mask bit set':<{max_key_length}} : {mask_bit_count}")
    print(f"{'Entries with default ACL set':<{max_key_length}} : {default_acl_count}")
    print(f"{'Total number of entries processed':<{max_key_length}} : {entry_count}")
