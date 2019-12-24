"""
Spark-related file-handling utilities.
"""

import contextlib

HTTP = "http://"
HTTPS = "https://"
BLOB = "blob:"


def files_to_rdd(context,
                 num_cores=1,
                 data_file="data.txt"):
    """
    Populate Spark RDD with file names or URLs over which a query is
    to be run.

    :param context: Spark Context
    :type context: pyspark.context.SparkContext
    :param num_cores: number of cores over which to parallelize Spark
    job
    :type num_cores: int
    :param data_file: name of file with file names or URLs, one per
    line
    :type data_file: str or unicode
    :return: RDD
    :rtype: pyspark.rdd.RDD
    """
    rdd_filenames = context.textFile(data_file, num_cores)
    return rdd_filenames


@contextlib.contextmanager
def open_stream(filename):
    """
    Open a file and return a stream to the file.

    If filename starts with "http:" or "https:" then file is assumed
    to be a URL.

    If filename starts with "blob:" then file is assumed to be held
    within Azure as a BLOB. This expects the following environment
    variables to be set:

    * BLOB_SAS_TOKEN
    * BLOB_ACCOUNT_NAME
    * BLOB_CONTAINER_NAME

    Otherwise, the filename is assumed to be held on the file
    system.

    :param filename: file name or URL
    :type filename: str or unicode
    :return: open stream
    :rtype: cStringIO.StringI (URL or file system) OR io.BytesIO (blob)
    """
    assert filename, "Filename must not be ''"

    is_url = (filename.lower().startswith(HTTP) or
              filename.lower().startswith(HTTPS))
    is_blob = (filename.lower().startswith(BLOB))

    if is_url:
        import requests
        from io import StringIO

        stream = requests.get(filename, stream=True).raw
        stream.decode_content = True
        stream = StringIO(stream.read())
        yield stream

    elif is_blob:
        import io
        import os
        from azure.storage.blob import BlobClient

        filename = filename[len(BLOB):]
        if ('BLOB_SAS_CONNECTION_STRING' not in os.environ 
            or 'BLOB_CONTAINER_NAME' not in os.environ):
            raise Exception('Missing required environment variables: '
                'BLOB_SAS_CONNECTION_STRING and BLOB_CONTAINER_NAME')
        connection_string = os.environ['BLOB_SAS_CONNECTION_STRING']
        container_name = os.environ['BLOB_CONTAINER_NAME']
        blob_client = BlobClient.from_connection_string(
            connection_string,
            container_name,
            filename)
        # this seems dangerous: downloading the complete file into memory
        stream = io.BytesIO(blob_client.download_blob().content_as_bytes())
        yield stream
    else:
        with open(filename) as f:
            yield f

    return stream
