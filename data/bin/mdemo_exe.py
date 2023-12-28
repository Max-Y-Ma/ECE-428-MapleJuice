import os
import sys

"""
    MapleJuice Demo Query:

    Dataset: CSVs

    Maple Phase:
        - For each "interconne" of type 'X' output:
            - <1, "Detection_">

    Juice Phase:
        - Take file with key 1, sum up all tallies:
            - <"Detection_", percentage>
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

def maple_filter(input_string):
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

def maple_output(keys_list):
    """
    Prints generated keys to stdout. This is the output
    format of the maple framework.
    """
    for n in range(len(keys_list)):
        if n == (len(keys_list) - 1):
            print(f"{keys_list[n]}", end="")
        else:
            print(f"{keys_list[n]}", end=" ")

""" Maple Framework Code End """

def maple_task(infile, prefix, X_arg):
    """
    Returns a list of keys from a given input file
    """

    output_file_prefix = f".{prefix}"
    keys_list = []

    # Open the file in read mode ('r')
    with open(infile, 'r') as csvfile:

        # Universal key for lines matching regular expression
        key = "1"

        # Output file path
        file_path = f"{FTP_DIRECTORY}/{EXE_DIRECTORY}/{output_file_prefix}_{key}"
        
        # Split the file into lines
        lines_list = csvfile.readlines()

        # "Detection_" csv index is number 9
        detection_index = 9 
        # "Interconne" csv index is number 10
        interconne_index = 10

        # Check if line matches regular expression
        for line in lines_list:
                
                # Split the csv lines into fields
                fields_list = line.split(',')
                if len(fields_list) < interconne_index or fields_list[0] == "\n":
                    continue

                entry = fields_list[interconne_index]

                # Verify entry matches X_args
                if entry != X_arg:
                    continue

                if not os.path.isfile(file_path):
                    # Open the file in write mode ('w') if it doesn't exist
                    with open(file_path, 'w') as wfile:
                        # Write content to the file
                        wfile.write(key_value_pair_formatter(key, fields_list[detection_index]))

                    keys_list.append(key)
                else:
                    # Open the file in append mode ('a')
                    with open(file_path, 'a') as afile:
                        # Append content to the file
                        afile.write(key_value_pair_formatter(key, fields_list[detection_index]))

    return keys_list

if __name__ == "__main__":
    # Grab arguments
    input_file = sys.argv[1]
    sdfs_prefix = sys.argv[2]
    X_arg = sys.argv[3]

    # Execute task and return
    out_keys = maple_task(input_file, sdfs_prefix, X_arg)
    maple_output(out_keys)
