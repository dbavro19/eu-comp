import boto3
import botocore
from langchain_community.document_loaders import PyPDFLoader
import json
from opensearchpy import OpenSearch
from opensearchpy import RequestsHttpConnection, OpenSearch, AWSV4SignerAuth




def get_embeddings(bedrock, text):
    body_text = json.dumps({"inputText": text})
    modelId = 'amazon.titan-embed-text-v1'
    accept = 'application/json'
    contentType='application/json'

    response = bedrock.invoke_model(body=body_text, modelId=modelId, accept=accept, contentType=contentType)
    response_body = json.loads(response.get('body').read())
    embedding = response_body.get('embedding')

    return embedding

def index_doc(client, vectors, content, source, page_number, link):

    try:
        page = int(page_number)+1
    except:
        page = 0

    indexDocument={
        'vectors': vectors,
        'content': content,
        'link' : link,
        'source': source,
        'page': page
        }

    response = client.index(
        index = "posttrade", #Use your index 
        body = indexDocument,
    #    id = '1', commenting out for now
        refresh = False
    )
    return response

#Manually specify doc and link for now
#loader = PyPDFLoader("CELEX_32017R0590_EN_TXT.pdf")
link = "https://www.esma.europa.eu/sites/default/files/library/2016-1452_guidelines_mifid_ii_transaction_reporting.pdf"
loader = PyPDFLoader("2016-1452_guidelines_mifid_ii_transaction_reporting.pdf")
pages = loader.load_and_split()


config = botocore.config.Config(connect_timeout=300, read_timeout=300)
bedrock = boto3.client('bedrock-runtime' , 'us-east-1', config = config)

#Setup Opensearch connectionand clinet
host = '14dzfsbbbt70yuz57f23.us-west-2.aoss.amazonaws.com' #use Opensearch Serverless host here
region = 'us-west-2'# set region of you Opensearch severless collection
service = 'aoss'
credentials = boto3.Session().get_credentials() #Use enviroment credentials
auth = AWSV4SignerAuth(credentials, region, service) 

oss_client = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20
)

for page in pages:
    content = page.page_content
    metadata  = page.metadata

    source = metadata["source"]
    page_number = metadata["page"]

    embeddings = get_embeddings(bedrock, content)

    response = index_doc(oss_client, embeddings, content, source, page_number, link)
    
    print("Page: " + str(page_number) + " Done")






