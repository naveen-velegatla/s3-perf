import boto3
import json
import time
import os


def launch_upload(no_of_obj):
    with open("sample_file.txt", "w") as f:
        f.seek((1024 * 1024) - 1)
        f.write("This is sample file")
    file_size = os.path.getsize("sample_file.txt")

    with open('configuration.json', 'r') as fr:
        configuration = json.loads(fr.read())

    s3 = boto3.resource('s3',
                        endpoint_url=configuration['endpoint_url'],
                        aws_access_key_id=configuration['access_key'],
                        aws_secret_access_key=configuration['secret_key'])

    for i in range(no_of_obj):
        s3.Bucket(configuration['bucket_name']).\
            upload_file("sample_file.txt", "uploaded_sample_file{}.txt".format(i))

    os.remove("sample_file.txt".format(i))
    return file_size


def main():
    no_of_obj = 10
    start_time = time.time()
    file_size = launch_upload(no_of_obj)
    elapsed_time = time.time() - start_time

    print("Elapsed time: {:.2f} Sec".format(elapsed_time))
    total_downloaded_size_in_mbit = ((file_size * 8 * no_of_obj) / 1024) / 1024
    print("Data Written/Uploaded: {:.3f} MBit/sec".format(total_downloaded_size_in_mbit / elapsed_time))


main()
