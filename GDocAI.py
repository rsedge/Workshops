from google.cloud import documentai_v1beta2 as documentai
import os
from PIL import Image
import pandas as pd
from google.cloud import storage
import json
import re
import sys
import img2pdf


def configure():
    """
    set up google service account autentication json file, project id, upload bucket name and source file path
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 
    pd.set_option("display.max_columns", 1000)
    pd.set_option('display.expand_frame_repr', False)
    global BUCKET_NAME, PROJECT_ID, DOC_PATH
    BUCKET_NAME = 
    PROJECT_ID = 
    DOC_PATH = './Docs'


def list_blobs():
    """
    Lists all the blobs in the bucket
    """
    storage_client = storage.Client()
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(BUCKET_NAME)
    return [i.name for i in blobs]


def org_df(df):
    df = df.replace('\n', ' ', regex=True)
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header
    return df


def clean(row):
    return ''.join([s.replace(' ', '').replace('\n', '') for s in row])


def is_numeric(s):
    if re.match('[\\d,.%/\\$]+', s):
        return True
    return False


def scan_type(row):
    tmp = [is_numeric(s) for s in row]
    if (tmp.count(True) > tmp.count(False)) or ('' in row):
        return 'content'
    return 'header'


def parse_table(file_name):
    """
    Parse uploaded file with document ai
    """
    client = documentai.DocumentUnderstandingServiceClient()
    gcs_source = documentai.types.GcsSource(uri=get_uri(file_name))

    input_config = documentai.types.InputConfig(
        gcs_source=gcs_source, mime_type='application/pdf')

    table_bound_hints = [
        documentai.types.TableBoundHint(
            page_number=1,
            bounding_box=documentai.types.BoundingPoly(
                # Define a polygon around tables to detect
                # Each vertice coordinate must be a number between 0 and 1
                normalized_vertices=[
                    # Top left
                    documentai.types.geometry.NormalizedVertex(
                        x=0,
                        y=0
                    ),
                    # Top right
                    documentai.types.geometry.NormalizedVertex(
                        x=1,
                        y=0
                    ),
                    # Bottom right
                    documentai.types.geometry.NormalizedVertex(
                        x=1,
                        y=1
                    ),
                    # Bottom left
                    documentai.types.geometry.NormalizedVertex(
                        x=0,
                        y=1
                    )
                ]
            )
        )
    ]
    table_extraction_params = documentai.types.TableExtractionParams(
        enabled=True,
        # model_version= "builtin/latest",
        table_bound_hints=table_bound_hints,
        # header_hints = str(['3048 '])
    )
    parent = 'projects/{}/locations/us'.format(PROJECT_ID)
    request = documentai.types.ProcessDocumentRequest(parent=parent,
                                                      input_config=input_config,
                                                      table_extraction_params=table_extraction_params
                                                      )
    document = client.process_document(request=request)

    # for batch processing
    # operation = client.batch_process_documents(batch_request)

    def _get_text(el):
        """
        Doc AI identifies form fields by their offsets
        in document text. This function converts offsets
        to text snippets.
        """
        response = ''
        # If a text segment spans several lines, it will
        # be stored in different text segments.
        for segment in el.text_anchor.text_segments:
            start_index = segment.start_index
            end_index = segment.end_index
            response += document.text[start_index:end_index]
        return response

    stacked_list = []
    for page in document.pages:
        for table_num, table in enumerate(page.tables):
            for row_num, row in enumerate(table.header_rows):
                stacked_list.append([_get_text(cell.layout) for cell in row.cells])

            for row_num, row in enumerate(table.body_rows):
                stacked_list.append([_get_text(cell.layout) for cell in row.cells])

    return extract(stacked_list)


def upload_blob(source_file_name, destination_blob_name):
    """
    Upload a file to google storage bucket if it's pdf
    else convert image to pdf and upload
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    orig_file = DOC_PATH + "/" + source_file_name
    convert_name = orig_file

    if( source_file_name.endswith(".jpg") ):
        with open(orig_file.replace(".jpg",".pdf"), "wb") as f:
            f.write(img2pdf.convert(orig_file))
        convert_name = orig_file.replace(".jpg",".pdf")
        destination_blob_name = destination_blob_name.replace(".jpg",".pdf")
    elif( source_file_name.endswith(".jpeg") ):
        with open(orig_file.replace(".jpeg",".pdf"), "wb") as f:
            f.write(img2pdf.convert(orig_file))
        convert_name = orig_file.replace(".jpeg",".pdf")
        destination_blob_name = destination_blob_name.replace(".jpeg",".pdf")
    elif( source_file_name.endswith(".png") ):
        with open(orig_file.replace(".png",".pdf"), "wb") as f:
            f.write(img2pdf.convert(orig_file))
        convert_name = orig_file.replace(".png",".pdf")
        destination_blob_name = destination_blob_name.replace(".png",".pdf")

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(convert_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )
    
    return destination_blob_name


def upload_cloud(filename):
    """
    file should be in the working directory
    """
    files_in_server = list_blobs()
    if filename in files_in_server:
        print('file already exists in Cloud')
        return filename
    else:
        return upload_blob(filename, filename)


def get_uri(file_name):
    """
    get uri for file
    """
    return f'gs://{BUCKET_NAME}/{file_name}'


def extract(stacked_list):
    """
    extract raw document ai table output and store them into dataframe
    """
    hist = []
    master_dict_list = []
    K = 0

    #  calculate different matrics to classify rows to different tables
    for counter, row in enumerate(stacked_list):
        tmp_dict = pd.DataFrame({'item': '\t'.join(row)}, index=[counter])
        tmp_dict['new_colnum'] = len(row) != K
        tmp_dict['size'] = len(row)
        tmp_dict['type'] = scan_type(row)
        cleaned_row = clean(row)
        if cleaned_row in hist:
            tmp_dict['repeat'] = True
            tmp_dict['hist_ref'] = hist.index(cleaned_row)
        else:
            tmp_dict['repeat'] = False
            hist.append(cleaned_row)
            tmp_dict['hist_ref'] = hist.index(cleaned_row)
        K = len(row)
        master_dict_list.append(tmp_dict)

    master_df = pd.concat(master_dict_list, axis=0).reset_index().rename(columns={'index': 'l_ref'})
    master_df['max_size'] = master_df.groupby(['hist_ref'])['size'].transform('max')
    master_df['l_first_appear'] = master_df.groupby(['hist_ref'])['l_ref'].transform('min')
    master_df['table_group'] = (master_df['type'] == 'header').cumsum()
    table_ref_dict = {}  # a temp dict used to conduct comparison process for overlaps
    fin_df_dict = {}  # final  data frame dict

    for group_id, table_df0 in master_df.groupby('table_group'):
        # filter content rows to conduct following steps to avoid excluding two different tables with same header
        table_ref_dict[group_id] = table_df0.loc[table_df0['type'] == 'content', 'hist_ref']

    key = 0
    for i, table_df0 in master_df.groupby('table_group'):
        repeated_table_size = [table_ref_dict[j].shape[0] for j in table_ref_dict if
                               set(table_ref_dict[i]) & set(table_ref_dict[j]) and j != i]
        if (table_df0.shape[0] > 1):# and (not repeated_table_size):
            fin_df_dict[key] = org_df(pd.DataFrame([stacked_list[x] for x in table_df0['l_ref']]))
            key += 1
        elif repeated_table_size:
            if table_df0.shape[0] > max(repeated_table_size):
                fin_df_dict[key] = org_df(pd.DataFrame([stacked_list[x] for x in table_df0['l_ref']]))
                key += 1

    return fin_df_dict


def create_bucket_class_location():
    """
    Utility function - NOT USED
    Create a new bucket in specific location with storage class
    """
    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket
