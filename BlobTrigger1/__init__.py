import os
import tempfile
import zipfile
import gzip
import azure.functions as func

def main(myblob: func.InputStream, outputBlob: func.Out[str]) -> None:
    blob_name = myblob.name
    if blob_name.endswith('.gz'):
        process_gzip_blob(myblob, outputBlob)
    elif blob_name.endswith('.zip'):
        process_zip_blob(myblob, outputBlob)

def process_gzip_blob(input_blob, output_blob):
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        # Extract the GZIP file
        with gzip.open(tmp_file.name, 'wb') as gz_file:
            gz_file.write(input_blob.read())

        # Get the extracted data
        tmp_file.seek(0)
        extracted_data = tmp_file.read()

        # Determine the destination path for the CSV file
        dest_blob_name = input_blob.name.replace('.gz', '.csv')

        # Upload the extracted data as a CSV file
        output_blob.set(extracted_data, content_type='text/csv', name=dest_blob_name)

        # Clean up the temporary file
        os.remove(tmp_file.name)

def process_zip_blob(input_blob, output_blob):
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Extract the ZIP file
        zip_file_path = os.path.join(tmp_dir, input_blob.name)
        with open(zip_file_path, 'wb') as file:
            file.write(input_blob.read())

        # Get the extracted CSV files
        extracted_files = []
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                if member.endswith('.csv'):
                    extracted_files.append(member)

        # Upload each extracted file as a CSV
        for extracted_file in extracted_files:
            with zip_ref.open(extracted_file) as csv_file:
                # Determine the destination path for the CSV file
                dest_blob_name = extracted_file

                # Upload the CSV file
                output_blob.set(csv_file.read(), content_type='text/csv', name=dest_blob_name)

if __name__ == "__main__":
    main(func.InputStream(""), func.Out[str](""))
