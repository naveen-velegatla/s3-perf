import boto3
import json
import time
import os


def launch_download(no_of_obj):
    with open('configuration.json', 'r') as fr:
        configuration = json.loads(fr.read())

    s3 = boto3.resource('s3',
                        endpoint_url=configuration['endpoint_url'],
                        aws_access_key_id=configuration['access_key'],
                        aws_secret_access_key=configuration['secret_key'])

    for i in range(no_of_obj):
        s3.Bucket(configuration['bucket_name']).\
            download_file("uploaded_sample_file{}.txt".format(i), "downloaded_sample_file{}.txt".format(i))

    file_size = os.path.getsize("downloaded_sample_file1.txt")
    return file_size


def main():
    no_of_obj = 10

    start_time = time.time()
    file_size = launch_download(no_of_obj)
    elapsed_time = time.time() - start_time

    print("Elapsed time: {:.2f} Sec".format(elapsed_time))
    total_downloaded_size_in_mbit = ((file_size * 8 * no_of_obj) / 1024) / 1024
    print("Data Read/Downloaded: {:.3f} MBit/sec".format(total_downloaded_size_in_mbit / elapsed_time))

main()



