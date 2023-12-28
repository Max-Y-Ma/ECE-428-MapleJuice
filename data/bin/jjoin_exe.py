import os
import sys
import ast

"""
    MapleJuice Join SQL Query:

    Dataset: CSVs

    Maple Phase: Run this each dataset
        - For each entry in field column in the CSV, output a KV pair:
            - <entry, <dataset_id, line>>

    Juice Phase:
        - For each field, sum lines from each dataset and output:
            - <lines_d1, lines_d2>
"""

"""
    Juice Framework:

    Inputs:
        - List of keys
        - SDFS filename prefix
        - SDFS destination filename

    Outputs: 
        - Append all output to sdfs_dest_filename
"""

""" Juice Framework Code Start """

FTP_DIRECTORY = f"{os.environ.get('PWD')}/data"
EXE_DIRECTORY = "bin"

def key_value_pair_formatter(key, value):
    """ Format key value pair with delimiter"""
    return f"{key}*{value}\n"

def juice_filter(input_string):
    """
    Remove special characters from the input string.

    Parameters:
    - input_string (str): The input string from which characters will be removed.
    - characters_to_remove (str): A string containing the characters to be removed.

    Returns:
    - str: The input string with specified characters removed.
    """
    result = ""
    special_characters = [
        '!', '"', '#', '$', '%', '&', "'", '(', ')', '*',
        '+', ',', '-', '.', '/', ':', ';', '<', '=', '>',
        '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|',
        '}', '~', '\n', ' '
    ]
    for char in input_string:
        if char not in special_characters:
            result += char
    return result

""" Juice Framework Code End """

def juice_task(keys, sdfs_prefix, sdfs_dest_filename):
    """
    Reads file sdfs_prefix_K and sums all the values for each key K.
    Writes or appends <word/number, sum(V)> to sdfs_dest_filename.
    """

    # Generate file names
    file_names = [f".{sdfs_prefix}_{key}" for key in keys]

    # Output to sdfs_dest_filenam in EXE Directory
    with open(f"{FTP_DIRECTORY}/{EXE_DIRECTORY}/.{sdfs_dest_filename}", 'w') as outfile:
        for file_name in file_names:
            key = ""
            lines_d1 = ""
            d1_id = -1
            lines_d2 = ""
            d2_id = -1

            # Determine to read local cached sfds file or local file
            if not os.path.isfile(f"{FTP_DIRECTORY}/{file_name}"):
                file_name = f"_{sdfs_prefix}_{file_name.split('_')[-1]}"

            # Open SDFS prefix_K files 
            with open(f"{FTP_DIRECTORY}/{file_name}", 'r') as key_file:
                key_value_pairs = key_file.readlines()

                for key_value_pair in key_value_pairs:
                    # Process key values pair
                    key_value_pair = key_value_pair.split('*')

                    if len(key_value_pair) < 2:
                        continue

                    key = key_value_pair[0]

                    value = key_value_pair[1].split('$')
                    dataset_id = value[0]
                    line = value[1]

                    # Initialize IDs dynamically
                    if d1_id == -1 and dataset_id != d2_id:
                        d1_id = dataset_id
                    elif d2_id == -1  and dataset_id != d1_id:
                        d2_id = dataset_id

                    # Append appropriate lines from each dataset
                    if dataset_id == d1_id:
                        lines_d1 += line.replace("\n", "")
                        continue
                    
                    if dataset_id == d2_id:
                        lines_d2 += line.replace("\n", "")
                        continue

            # Output key value pair
            if lines_d1 != "" and lines_d2 != "":
                outfile.write(key_value_pair_formatter(key, f"{lines_d1}${lines_d2}"))

if __name__ == "__main__":
    # Grab arguments
    keys = ast.literal_eval(sys.argv[1])
    sdfs_prefix = sys.argv[2]
    sdfs_dest_filename = sys.argv[3]

    # Execute task and return
    juice_task(keys, sdfs_prefix, sdfs_dest_filename)