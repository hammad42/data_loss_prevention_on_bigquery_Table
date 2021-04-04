def dlp(request):
  from google.cloud import bigquery
  import os
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:\gcp_credentials\elaborate-howl-285701-105c2e8355a8.json"
  client_bigquery = bigquery.Client()#bigquery client
  import uuid
  import google.cloud.dlp
  import time
  
  
  uuid=str(uuid.uuid4())
  print(uuid)
  request_json = request.get_json()#json message received from http request

  if request_json:
    file_name=request_json["file_name"]
    print(file_name)
    #query of creating table start
    
  query="""

      create table `elaborate-howl-285701.context.{uuid}_dlp` as SELECT * FROM `elaborate-howl-285701.context.form_key_pair` 
  where file_name=\"{file_name}\";
  """.format(uuid=uuid,file_name=file_name)
  #query of creating table end
  job_config = bigquery.QueryJobConfig()
  query_job = client_bigquery.query(query, location="US", job_config=job_config)
  query_job.result()
  #dlp work start

  project='elaborate-howl-285701'
  bigquery_project='elaborate-howl-285701'
  dataset_id='context'
  table_id=uuid+'_dlp'
  min_likelihood=None,
  max_findings=None,
  parent = f"projects/{project}/locations/global"

  inspect_job_data = {
      'storage_config': {
          
          'big_query_options': {
              'table_reference': {
                  
                  'project_id': bigquery_project,
                  'dataset_id': dataset_id,
                  'table_id': table_id
                  
              },
              'identifying_fields':[
                  {
                    'name':'file_name',
                  }
              ],
              'excluded_fields':[
                  {
                    'name':'field_name',
                    'name':'time_stamp',
                    'name':'validated_field_name',
                    'name':'validated_field_value',
                    'name':'updated_date',
                    'name':'confidence',
                    'name':'updated_by',
                    'name':'key_x1',   
                    'name':'key_x2',
                    'name':'key_y1',
                    'name':'key_y2',
                    'name':'value_x1', 
                    'name':'value_x2',   
                    'name':'value_y1',
                    'name':'value_y2',
                    'name':'pageNumber',
                    'name':'id', 
                    'name':'type'

                  }
              ],
              
              'rows_limit':10000,
              'sample_method':'TOP',
          },
      },
      'inspect_config': {
          'info_types': [{'name': 'FIRST_NAME'}, {'name': 'LAST_NAME'}, {'name': 'EMAIL_ADDRESS'},{'name': 'AGE'}, {'name': 'CREDIT_CARD_NUMBER'}, {'name': 'DATE'},{'name': 'DATE_OF_BIRTH'}, {'name': 'DOMAIN_NAME'}, {'name': 'EMAIL_ADDRESS'},
           {'name': 'US_EMPLOYER_IDENTIFICATION_NUMBER'}, {'name': 'US_INDIVIDUAL_TAXPAYER_IDENTIFICATION_NUMBER'},{'name': 'US_PREPARER_TAXPAYER_IDENTIFICATION_NUMBER'}, {'name': 'US_SOCIAL_SECURITY_NUMBER'}, {'name': 'US_VEHICLE_IDENTIFICATION_NUMBER'},
           {'name': 'US_TOLLFREE_PHONE_NUMBER'}, {'name': 'US_STATE'}, {'name': 'US_PASSPORT'},{'name': 'US_HEALTHCARE_NPI'}, {'name': 'GENDER'}, {'name': 'LOCATION'}, {'name': 'PASSPORT'}, {'name': 'PASSWORD'},
            {'name': 'PHONE_NUMBER'}, {'name': 'STREET_ADDRESS'},{'name': 'URL'}, {'name': 'US_BANK_ROUTING_MICR'}, {'name': 'US_DEA_NUMBER'},{'name': 'US_DRIVERS_LICENSE_NUMBER'}],
          "include_quote": True,
          "min_likelihood": 2,
      },
      'actions': [
          {
              'save_findings': {
                  'output_config':{
                      'table':{
                          'project_id': bigquery_project,
                          'dataset_id': dataset_id,
                          'table_id': '{}_job'.format(table_id)
                      }
                  }
                  
              },
          },
      ]
  }
  dlp = google.cloud.dlp_v2.DlpServiceClient()
  operation = dlp.create_dlp_job(parent=parent, inspect_job=inspect_job_data)

  time.sleep(200)



  #dlp work end
  #query for dropping created table
  query2="""
  drop table  `elaborate-howl-285701.context.{table_id}`;
    
  """.format(table_id=table_id)
  #query of creating table end
  job_config = bigquery.QueryJobConfig()
  query_job2 = client_bigquery.query(query2, location="US", job_config=job_config)
  query_job2.result()

  #checking rows in form_key_pair table

  destination_table = client_bigquery.get_table('elaborate-howl-285701.context.form_key_pair_dlp')  # Make an API request.
  print("before insertion {} rows.".format(destination_table.num_rows))



  #copy data loss prevention on desired form_key_pair_dlp
  query3="""
  INSERT INTO `elaborate-howl-285701.context.form_key_pair_dlp`
  SELECT * FROM `elaborate-howl-285701.context.{tableid2}_job`
  """.format(tableid2=table_id)
  print(query3)
  #query of creating table end
  job_config = bigquery.QueryJobConfig()
  query_job3 = client_bigquery.query(query3, location="US", job_config=job_config)
  query_job3.result()
  #time.sleep(30)
  #checking rows in form_key_pair table

  destination_table = client_bigquery.get_table('elaborate-howl-285701.context.form_key_pair_dlp')  # Make an API request.
  print("after insertion {} rows.".format(destination_table.num_rows))

  job = dlp.get_dlp_job(request={"name": operation.name})
  result_count=""
  if job.inspect_details.result.info_type_stats:

    for finding in job.inspect_details.result.info_type_stats:

        result_="Info type: {}; Count: {}".format(finding.info_type.name, finding.count)
        result_count=result_+result_count+'\n'
        print(result_count)


  #query for dropping dlp table
  query4="""
  drop table  `elaborate-howl-285701.context.{table_id2}_job`;
  """.format(table_id2=table_id)
  #query of creating table end
  job_config = bigquery.QueryJobConfig()
  query_job4 = client_bigquery.query(query4, location="US", job_config=job_config)
  query_job4.result()

  ## work for neo4j starts

  query5 = """
    select distinct  a.field_value, a.field_name, b.info_type.name as info_, b.likelihood from `elaborate-howl-285701.context.form_key_pair` a,
    `elaborate-howl-285701.context.form_key_pair_dlp` b
    where a.file_name=\"{file_name}\"
    and lower(a.field_value)=lower(b.quote);
    """.format(file_name=file_name)
  query_job5 = client_bigquery.query(
      query5,
    # Location must match that of the dataset(s) referenced in the query.
      location="US",
  )  # API request - starts the query

  df = query_job5.to_dataframe()
  f_value=[]
  for a in df.field_value:
    f_value.append(a)

  f_name=[]
  for b in df.field_name:
    f_name.append(b)

  info_name=[]
  for c in df.info_:
    info_name.append(c)    
  from neo4j import GraphDatabase
  import logging
  from neo4j.exceptions import ServiceUnavailable
  class App:


    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    def close(self):
            # Don't forget to close the driver connection when you are finished with it
            self.driver.close()
    def create_friendship(self,file_name,field_value,field_name,info_):
            with self.driver.session() as session:
                # Write transactions allow the driver to handle retries and transient errors
                result = session.write_transaction(
                    self._create_and_return_friendship, file_name, field_value,field_name,info_)
                print(result)
                #for row in result:
                #    print("Created relation between: {n}, {m} ".format(n=row['n'], m=row['m']))
                #    print("Created relation between: {n}, {e} ".format(n=row['n'], e=row['e']))
                #    print("Created relation between: {e}, {m} ".format(e=row['e'], m=row['m']))
                #   print("Created relation between: {m}, {w} ".format(m=row['m'], w=row['w']))
    @staticmethod
    def _create_and_return_friendship(tx, file_name, field_value,field_name,info_):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = """
        merge (n:File {Name: $file_name})
        merge (m:FIELD {Name: $field_name})
        merge (e:VALUE {value: $field_value})
        merge (w:DLP_Classification {NAME: $info_})

        merge (n)-[p:CONTAINS_FIELD]->(m)
        merge (n)-[q:CONTAINS_VALUE]->(e)
        merge (e)-[r:TYPE_IS]->(m)
        merge (m)-[s:DATA_Classification]->(w)


        
        RETURN n, m, e, w, p, q, r, s
        """
        result = tx.run(query, file_name=file_name, field_value=field_value,field_name=field_name,info_=info_)
        try:
            return [{"n": row["n"]["name"], "e": row["e"]["address"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
  import itertools
  for (a,b,c) in zip(f_value,f_name,info_name):
    print(a+','+b+','+c)
    bolt_url = "neo4j+s://cfb079ca.databases.neo4j.io"
    user = "neo4j"
    password = "hqr34pE89cwcrVxyYM6IpUmWjSZfK3pQ7D02i5nwe44"
    app = App(bolt_url, user, password)
    app.create_friendship(file_name, a,b,c)
    app.close()


  

  return "df"


