# Execute the data pipeline
python ./main/project/AutoPipeLine.py

data_file="./main/data/zylesto.sqlite"


# Check if the output file exists
if [ -f "$data_file" ]; then
    echo "Yes! Output file $data_file is available."
else
    echo "No! Output file $data_file is not available."
fi