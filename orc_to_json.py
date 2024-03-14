import json
import io
import pyorc
from org.apache.nifi.processor.io import StreamCallback


# Subclass StreamCallback to handle FlowFiles
class PyStreamCallback(StreamCallback):
    def __init__(self):
        self.contents = None

    def process(self, inputStream, outputStream):
        # Read the content of the incoming FlowFile
        content = io.BytesIO(inputStream.read())

        # Create an ORC reader object
        reader = pyorc.Reader(content)

        # Initialize an empty list to hold JSON objects
        json_data = []

        # Iterate over each row in the ORC file
        for row in reader:
            # Convert the row to a dictionary
            row_dict = dict(zip(reader.schema.fields, row))
            # Append the dictionary to the list
            json_data.append(row_dict)

        # Convert the list of dictionaries to JSON format
        json_output = json.dumps(json_data)

        # Write the JSON output to the output stream
        outputStream.write(bytearray(json_output, "utf-8"))


# Create a new instance of PyStreamCallback
callback = PyStreamCallback()

# Create a FlowFile session
flowFile = session.get()

if flowFile is not None:
    # Use the session to read the content of the FlowFile and apply the callback
    session.read(flowFile, callback)
    session.write(flowFile, callback)
    # Finish the processing of the FlowFile
    session.transfer(flowFile, REL_SUCCESS)
