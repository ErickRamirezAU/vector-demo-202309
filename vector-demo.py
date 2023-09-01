from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra.query import SimpleStatement
import openai
import numpy
import pandas as pd
import time
from datasets import load_dataset

openai.api_key = OPENAI_API_KEY
cass_user = 'token'
cass_pw = ASTRA_DB_TOKEN
keyspace = 'vectordb'

cloud_config= {
  'secure_connect_bundle': SCB_PATH
}
auth_provider = PlainTextAuthProvider(cass_user, cass_pw)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
session = cluster.connect()
session.set_keyspace(keyspace)

model_id = "text-embedding-ada-002"



# download "SQuAD" dataset from Hugging Face
data = load_dataset('squad', split='train')
data = data.to_pandas()
data.drop_duplicates(subset='context', keep='first', inplace=True)



# load data to Astra DB
counter = 0;
total = 0
for id, row in data.iterrows():

  converted_answers = dict()
  converted_answers['text'] = row.answers['text'][0]
  converted_answers['answer_start'] = str(row.answers['answer_start'][0])

  full_chunk = f"{row.context} {row.title}"
  embedding = openai.Embedding.create(
    input=full_chunk, model=model_id
    )['data'][0]['embedding']

  query = SimpleStatement(
    f"""
    INSERT INTO squad
    (id, title, context, question, answers, title_context_embedding)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    )
  session.execute(query, (row.id, row.title, row.context, row.question, converted_answers, embedding))

  counter += 1
  total += 1
  if(total >= 300):
    break
  if (counter >= 60):
    counter = 0;
    time.sleep(60)



# convert the user's question into a text embedding
user_question = "when was the college of engineering in the University of Notre Dame established?"

embedding = openai.Embedding.create(
  input=user_question, 
  model=model_id
  )['data'][0]['embedding']



# build a prompt for asking ChatGPT
message_objects = []
message_objects.append(
   {
      "role":"system",
      "content":"You're a chatbot helping customers with questions."
   })
message_objects.append(
   {
      "role":"user",
      "content": customer_input
   })

answers_list = []

# With the role as 'assistant',  load the results from Astra with Vector Search.  That helps the model to provide answer to the question asked by user.
for row in top_3_results:
    brand_dict = {'role': "assistant", "content": f"{row.context}"}
    answers_list.append(brand_dict)

message_objects.extend(answers_list)
message_objects.append({"role": "assistant", "content":"Here's my answer to your question."})

completion = openai.ChatCompletion.create(
  model="gpt-3.5-turbo",
  messages=message_objects
)

print(completion.choices[0].message['content'])
