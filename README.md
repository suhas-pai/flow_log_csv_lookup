# Flow Log CSV Lookup

Small project that takes a csv file and a flow-log txt file, and outputs useful information to a provided file

# Running

`uv` is recommended for this project. To run with `uv`, using the sample files, use the following command:
```
uv run main.py ./examples/sample.csv ./examples/sample-flow-log.txt output.txt
```

This project also supports running with only python3:
```
python3 run main.py ./examples/sample.csv ./examples/sample-flow-log.txt output.txt
```