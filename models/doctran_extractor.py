import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--video_id", required=True)
parser.add_argument("--channel_id", required=True)
args = parser.parse_args()

vid = args.video_id
cid = args.channel_id

import json
import time
import pandas as pd
import re
import os
import openai
from langchain.schema import Document
from doctran import Doctran, ExtractProperty
import pdb

openai.api_key = os.environ['OPENAI_API_KEY']

doctran = Doctran(openai_api_key=os.environ['OPENAI_API_KEY'], openai_model='gpt-3.5-turbo')

async def setup_transform_documents(documents, in_properties):
    
    ex_properties = [ExtractProperty(**property) for property in in_properties]
    for d in documents:
        
        doctran_doc = (
            await doctran.parse(content=d.page_content)
            .extract(properties=ex_properties)
            .execute()
        )
        
        d.metadata["extracted_properties"] = doctran_doc.extracted_properties
        
    
    return documents

async def atransform_paragraph(documents, in_properties):
    
    ex_properties = [ExtractProperty(**property) for property in in_properties]
    for d in documents:

        doctran_doc = (
            await doctran.parse(content=d.page_content)
            .summarize(token_limit=3500)
            .extract(properties=ex_properties)
            .execute()
        )
        
        d.metadata["extracted_properties"] = doctran_doc.extracted_properties
        d.metadata["transformed_content"] = doctran_doc.transformed_content
    
    return documents

async def qa_documents(documents):
    
    for d in documents:
        
        doctran_doc = (
            await doctran.parse(content=d.page_content)
            .interrogate()
            .execute()
        )
        
        d.metadata["extracted_qa"] = doctran_doc.extracted_properties
        
    
    return documents

class RequireKeyMissingException(Exception):
    pass

def create_doctran_df(process_document, type_chunks, time_chunks):
    df_process = pd.DataFrame(columns = ('video_id', 'channel_id', 'text', 'text_extract', 'text_category', 'timestamp'))

  
    indx = 0
    for ed in process_document:
        
        arr = "none"  
        text_category = "none"

        text_category = type_chunks[indx]
        timestamp = time_chunks[indx]

        if 'transformed_content' not in ed.metadata.keys():
            raise RequireKeyMissingException('missing ["transformed_content"] from json output')
            
        if 'extracted_properties' not in ed.metadata.keys():
            raise RequireKeyMissingException('missing ["extracted_properties"] from json output')
        else:
            if 'topics' not in ed.metadata['extracted_properties'] and 'steps' not in ed.metadata['extracted_properties']:
                text_category = 'other'
                
            # if 'topics' not in ed.metadata['extracted_properties']:
            #     raise RequireKeyMissingException('missing ["extracted_properties"]["topics"] from json output')

            # if 'steps' not in ed.metadata['extracted_properties']:
            #     raise RequireKeyMissingException('missing ["extracted_properties"]["steps"] from json output')

        summary = ed.metadata['transformed_content']
        
        if text_category == "step-process":
            arr = ed.metadata['extracted_properties']['steps']
            arr = '\n'.join([f'{str(i)}. {x}' for i, x in enumerate(arr)])
                
        if text_category == "topic-detail":
            arr = ed.metadata['extracted_properties']['topics']
            arr = '\n'.join([f'{str(i)}. {x}' for i, x in enumerate(arr)])
            
        
        df_process.loc[indx] = [vid, cid, summary, arr, text_category, timestamp]
        indx += 1

    return df_process

setup_properties = [
    {
        "name": "has_purpose",
        "description": "Does this text describe an object, process, or function.",
        "type": "boolean",
        "required": True,
    },
    {
        "name": "text_category",
        "description": "What category is this text in.",
        "type": "string",
        "enum": ['topic-detail', 'step-process', 'other'],
        "required": True,
    },
]

properties = [
    {
        "name": "steps",
        "description": "A list of all steps required to complete the task in this document.",
        "type": "array",
        "items": {
            "name": "step",
            "description": "The sentences that describe the step.",
            "type": "string",
        },
        "required": True,
    },
    {
        "name": "topics",
        "description": "A list of all described topics in this document.",
        "type": "array",
        "items": {
            "name": "topic",
            "description": "The sentences that describe the topic.",
            "type": "string",
        },
        "required": True,
    },
    {
        "name": "eli5",
        "description": "Explain this document to me like I'm 5 years old.",
        "type": "string",
        "required": True,
    },
]

first_letter_pattern = re.compile(r'(?<=[\.\?!]\s)(\w+)')
def capitalize(match):
    return(match.group().capitalize())

def chunk_list(inp, size=3):
    
    _chunks = [inp[x:x+size] for x in range(0, len(inp), size)]

    return _chunks

import asyncio
if __name__ == '__main__':

    loop = asyncio.get_event_loop()

    df = pd.read_csv(f'/root/data/video_transcript/{vid}.csv')

    video_timestamp = df['timestamp']
    video_transcript = df['text']

    documents = [Document(page_content=x) for x in video_transcript]

    try_count = 0
    while True:
        try:
            setup_document = asyncio.run(setup_transform_documents(
                documents, in_properties=setup_properties
            ))
            break
        except openai.error.ServiceUnavailableError as e:
            
            if try_count > 4:
                raise e
            
            try_count += 1
            
            time.sleep(5)
            continue

        except RequireKeyMissingException as e:
            
            if try_count > 4:
                raise e
                
            try_count += 1
            
            time.sleep(5)
            continue

        except openai.error.APIError as e:
            if e.code == 502:
                if try_count > 4:
                    raise e
                
                try_count += 1

                time.sleep(5)
                continue
            else:
                raise e
            
        except json.decoder.JSONDecodeError as e:
            if "Expecting property name enclosed in double quotes" in e.msg:
                if try_count > 4:
                    raise e
                
                try_count += 1

                time.sleep(5)
                continue
            else:
                raise e

        except Exception as e:
            raise e


    type_chunks = []
    process_chunks = []
    time_chunks = []
    steps = []
    timex = []
    prev_paragraph_type = None
    indx=0
    requires_steps = False
    for ed in setup_document:
        
        paragraph_type = ed.metadata['extracted_properties']['text_category']
        
        if paragraph_type == prev_paragraph_type and len(steps) > 0 and len(steps) < 3:

            steps.append(video_transcript[indx])
            timex.append(int(video_timestamp[indx]))
            
            prev_paragraph_type = paragraph_type

        elif len(steps) == 0:
            
            steps = [video_transcript[indx]]
            timex = [int(video_timestamp[indx])]
            prev_paragraph_type = paragraph_type
            
        else:
            
            process_chunks.append(steps)
            time_chunks.append(min(timex))
            type_chunks.append(paragraph_type)
            
            steps = [video_transcript[indx]]
            timex = [int(video_timestamp[indx])]
            prev_paragraph_type = paragraph_type
        
        indx+=1
        
    if len(steps)>0:
        process_chunks.append(steps)
        time_chunks.append(min(timex))
        type_chunks.append(paragraph_type)

    process_chunks = ['\n\n'.join(x) for x in process_chunks]

  
    documents = [Document(page_content=x) for x in process_chunks]


    try_count = 0
    saved_indx = 0
    df_process = []
    while True:

        
        
        try:
            doc_chunks = chunk_list(documents)
            _type_chunks = chunk_list(type_chunks)
            _time_chunks = chunk_list(time_chunks)

            for indx in range(saved_indx, len(doc_chunks)):
                process_document = asyncio.run(atransform_paragraph(
                    doc_chunks[indx], in_properties=properties
                ))
                df_tmp = create_doctran_df(process_document, _type_chunks[indx], _time_chunks[indx])
                df_process.append(df_tmp)
                
                saved_indx = indx
                
            df_process = pd.concat(df_process, axis=0)
            df_process.to_csv(f'/root/data/video_doctran/{vid}.csv', index=False)
            break
        except openai.error.ServiceUnavailableError as e:

            if try_count > 4:
                raise e
            
            try_count += 1

            time.sleep(5)
            continue

        except RequireKeyMissingException as e:
            
            if try_count > 4:
                raise e
            
            try_count += 1
            
            time.sleep(5)
            continue

        except json.decoder.JSONDecodeError as e:
            if "Expecting property name enclosed in double quotes" in e.msg:
                if try_count > 4:
                    raise e
                
                try_count += 1

                time.sleep(5)
                continue
            else:
                raise e

        except openai.error.APIError as e:
            if e.code == 502:
                if try_count > 4:
                    raise e
                
                try_count += 1

                time.sleep(5)
                continue
            else:
                raise e

        except Exception as e:
            raise e


    
  

    
