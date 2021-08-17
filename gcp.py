import csv
import apache_beam as beam
from apache_beam import Flatten, Create, ParDo, Map, pipeline
from apache_beam.io import ReadFromText, ReadAllFromText
from apache_beam.io import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.runners.common import unicode
from apache_beam.typehints import with_output_types
#git update

def print_row(element):
    print(element)

def parse_file(element):
  for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
    return line


class CollectData(beam.DoFn):
    def process(self, element):
        """
        Returns a list of tuples containing DeptNo, DepartmentName,Salary
        """
        result = [
            "{},{},{}".format(
                element[0],element[1][1][0],element[1][0][0])
        ]
        return result
class TupToDict(beam.DoFn):

    def process(self, element):
        di = {'DeptNo': element[0],  'DeptName': element[1][1][0], 'Sal': element[1][0][0]}
        return [di]


with beam.Pipeline() as p:
    emp = (p
           | "label1" >> beam.io.ReadFromText("gs://bucket2120/input/spark_emp_rdd_df_ds_data",skip_header_lines=1)
           #| 'ReadRequestFile' >> ReadFromText("spark_emp_rdd_df_ds_data.csv", skip_header_lines=1)
           | "parse1" >> beam.Map(parse_file)
           | beam.Map(lambda emp: (emp[3], int(emp[2])))
           | beam.CombinePerKey(lambda x: sum(x))
          # | 'Get max value' >> beam.CombineGlobally(lambda elements: max(elements or [-1]))
      #    | "JSON" >> beam.ParDo(TupToDict2())


     )
    dept = (p
            | "label2" >> beam.io.ReadFromText("gs://bucket2120/input/spark_dept_rdd_df_ds_data", skip_header_lines=1)
            | "parse2" >> beam.Map(parse_file)
            | beam.Map(lambda dept: (dept[0], dept[1]))
           #|"JSON0 1" >> beam.ParDo(TupToDict())

            )

    merged = (emp, dept) | beam.CoGroupByKey()  |"JSON" >>  beam.ParDo(TupToDict())
    project_id = "prime-heuristic-299502"  # replace with your project ID
    dataset_id = 'demo'  # replace with your dataset ID
    table_id = 'employee1'  # replace with your table ID
    table_schema2 = 'DeptNo:INTEGER, DeptName:STRING, Sal:INTEGER'

    merged | 'Write' >> beam.io.WriteToBigQuery(
        table=table_id,
        dataset=dataset_id,
        project=project_id,
        schema=table_schema2,
        custom_gcs_temp_location='gs://bucket2120/temp',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        batch_size=int(100)
    )

