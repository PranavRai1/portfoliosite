import azure.functions as func
import azure.durable_functions as df
import datetime
import json
import logging
import os
import tempfile
import zipfile
import uuid

from azure.identity import DefaultAzureCredential
from azure.storage.blob import ( 
    BlobServiceClient,
    ContentSettings
)



app = df.DFApp(
    http_auth_level=func.AuthLevel.ANONYMOUS
)

# ---------------------------------------------------------
# HTTP STARTER
# ---------------------------------------------------------

@app.route(
    route="start-blob-zip",
    methods=["POST"]
)
@app.durable_client_input(client_name="client")
async def start_blob_zip(
    req: func.HttpRequest,
    client
) -> func.HttpResponse:

    try:
        request_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Request body must be valid JSON.",
            status_code=400
        )

   # request_id = request_body.get("requestId")
    document_type = request_body.get("documenttype")
    isdigital = request_body.get("isdigital")
    district = request_body.get("district")

  #  if not request_id:
  #      return func.HttpResponse(
  #          "requestId is required.",
  #         status_code=400
  #     )

    instance_id = await client.start_new(
        "blob_zip_orchestrator",
        client_input=request_body
    )

    logging.info(
        f"Started orchestration with ID: {instance_id}"
    )

    return client.create_check_status_response(
        req,
        instance_id
    )


# ---------------------------------------------------------
# ORCHESTRATOR
# ---------------------------------------------------------

@app.orchestration_trigger(
    context_name="context"
)
def blob_zip_orchestrator(
    context: df.DurableOrchestrationContext
):

    request = context.get_input()

    # STEP 1
    # Find blobs using index tags
    blobs = yield context.call_activity(
        "find_blobs_activity",
        request
    )

    # STEP 2
    # Create a ZIP
    if not blobs:
        return {
            "status": "NoFilesFound",
            "blobCount": 0,
            "message": (
                "No blobs matched the supplied index tags."
            )
        }

    zip_result = yield context.call_activity(
        "create_zip_activity",
        blobs
    )

    return {
        "status": "Completed",
        "blobCount": len(blobs),
        "blobs": blobs,
        "zipResult": zip_result
    }


# ---------------------------------------------------------
# ACTIVITY 1
# Find blobs
# ---------------------------------------------------------

@app.activity_trigger(
    input_name="request"
)

def find_blobs_activity(
    request: dict
):


 #   request_id = request["requestId"]
    document_type = request["documenttype"]
    isdigital = request["isdigital"]
    district = request["district"]


    account_name = os.environ[
        "BUSINESS_STORAGE_ACCOUNT"
    ]

    container_name = os.environ[
        "BUSINESS_CONTAINER"
    ]

    account_url = (
        f"https://{account_name}.blob.core.windows.net"
    )

    credential = DefaultAzureCredential()

    blob_service_client = BlobServiceClient(
        account_url=account_url,
        credential=credential
    )

    container_client = (
        blob_service_client.get_container_client(
            container_name
        )
    )

    filter_expression = (
        f'"DocumentType" = \'{document_type}\' AND "IsDigitalCopy" = \'{isdigital}\' AND "District" = \'{district}\''
    )

    matching_blobs = (
        container_client.find_blobs_by_tags(
            filter_expression
        )
    )

    results = []

    for blob in matching_blobs:
        results.append(blob.name)

    return results




# ---------------------------------------------------------
# ACTIVITY 2
# Create ZIP
# ---------------------------------------------------------

@app.activity_trigger(
    input_name="blob_names"
)
def create_zip_activity(
    blob_names: list
):
    if not blob_names:
        raise ValueError(
            "No blobs were supplied to create_zip_activity."
        )

    account_name = os.getenv(
        "BUSINESS_STORAGE_ACCOUNT"
    )

    source_container_name = os.getenv(
        "BUSINESS_CONTAINER"
    )

    output_container_name = os.getenv(
        "OUTPUT_CONTAINER"
    )

    if not account_name:
        raise ValueError(
            "BUSINESS_STORAGE_ACCOUNT is not configured."
        )

    if not source_container_name:
        raise ValueError(
            "BUSINESS_CONTAINER is not configured."
        )

    if not output_container_name:
        raise ValueError(
            "OUTPUT_CONTAINER is not configured."
        )

    account_url = (
        f"https://{account_name}.blob.core.windows.net"
    )

    credential = DefaultAzureCredential()

    blob_service_client = BlobServiceClient(
        account_url=account_url,
        credential=credential
    )

    source_container = (
        blob_service_client.get_container_client(
            source_container_name
        )
    )

    output_container = (
        blob_service_client.get_container_client(
            output_container_name
        )
    )

    # Unique name prevents users/jobs overwriting one another
    zip_name = (
        f"documents-{uuid.uuid4()}.zip"
    )

    logging.info(
        f"Creating ZIP {zip_name} "
        f"containing {len(blob_names)} blobs."
    )

    #
    # TemporaryFile is backed by the Function worker's
    # temporary filesystem rather than storing the whole
    # ZIP as one Python bytes object.
    #
    with tempfile.TemporaryFile() as temp_zip:

        with zipfile.ZipFile(
            temp_zip,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            allowZip64=True
        ) as zip_file:

            for blob_name in blob_names:

                logging.info(
                    f"Adding blob to ZIP: {blob_name}"
                )

                source_blob_client = (
                    source_container.get_blob_client(
                        blob_name
                    )
                )

                downloader = (
                    source_blob_client.download_blob()
                )

                #
                # Preserve virtual folder structure
                # inside the ZIP.
                #
                zip_entry_name = (
                    blob_name.lstrip("/")
                )

                with zip_file.open(
                    zip_entry_name,
                    mode="w"
                ) as zip_entry:

                    for chunk in downloader.chunks():
                        zip_entry.write(chunk)

    #
        # zipfile leaves us at the end of the temporary file.
        # Move back to position 0 before uploading.
        #
        temp_zip.seek(0)

        output_blob_client = (
            output_container.get_blob_client(
                zip_name
            )
        )

        output_blob_client.upload_blob(
            temp_zip,
            overwrite=True,
            content_settings=ContentSettings(
                content_type="application/zip",
                content_disposition=(
                    f'attachment; filename="{zip_name}"'
                )
            )
        )

    logging.info(
        f"ZIP successfully uploaded: {zip_name}"
    )

    return {
        "storageAccount": account_name,
        "container": output_container_name,
        "blobName": zip_name,
        "fileCount": len(blob_names)
    }