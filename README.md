# tableau-hyper-union
A Python script to quickly union all Hyper extracts in a folder, into a single Hyper file. When using its executable form, you should be able to just drop it in a folder with extracts, run it, and find `union.hyper` containing the data from all extracts combined. Well, "unioned".

## Usage

Automatically operates on/unions all `.hyper` files in the directory _in which it is run_. Aside from that, its execution can be influenced with these parameters:

* `--output-file OUTPUT_FILE, -o OUTPUT_FILE`  
  The file to output to. Defaults to `union.hyper`.
* `--preserve-output-file, -p`  
  When this argument is specified, the script will preserve the output of the output file and append to the existing contents. When not specified (i.e. the default behavior), it will first clear the contents of said output file i.e. fully overwrite it.
* `--source-file-column-name SOURCE_FILE_COLUMN_NAME, -c SOURCE_FILE_COLUMN_NAME`  
  Used to add a column to each table, containing the name of the Hyper file the data was sourced from. The column can be omitted altogether by specifying an empty string here: "". Otherwise, the default is "source_file".
* `--log-to-file`  
  Log the output of the program to a log file, and not just to the console. Useful for when the tool is used on a schedule.

## Development Notes

* v0.1: severely under-tested and built just to solve a specific situation where files are known to have resemblant schema and table structure. Not identical, but similar. While the code includes _some_ logic to account for different schema and table structures, this hasn't been tested extensively.

## Executable
The Python script is rolled up into an executable with the following command:

```
pyinstaller -F --paths=.venv\Lib\site-packages --add-data="resources\*;.\resources" --add-binary=".venv\Lib\site-packages\tableauhyperapi\bin\hyper\hyperd.exe;tableauhyperapi\bin\hyper" --hidden-import=_cffi_backend --icon="resources\images\biztory.ico" tableau_hyper_union.py
```