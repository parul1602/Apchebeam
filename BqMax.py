import csv
import apache_beam as beam
from apache_beam import Flatten, Create, ParDo, Map, pipeline
from apache_beam.io import ReadFromText, ReadAllFromText
from apache_beam.io import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery



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
            di = {'firstname': element[0], 'lastname': element[1], 'salary': element[2], 'deptno': element[3]}
            return [di]

with beam.Pipeline() as p:
    emp = (p
           | "label1" >> beam.io.ReadFromText("gs://bucket2120/input/spark_emp_rdd_df_ds_data",skip_header_lines=1)
           | "parse1" >> beam.Map(parse_file)
           | "max">> beam.CombineGlobally(lambda elements: max(elements or [-1]))
           | "JSON" >>  beam.ParDo(TupToDict()) | beam.Map(print_row)

           )

    project_id = "prime-heuristic-299502"  # replace with your project ID
    dataset_id = 'demo'  # replace with your dataset ID
    table_id = 'employee1'  # replace with your table ID
    table_schema2 = 'DeptNo:INTEGER, DeptName:STRING, Sal:INTEGER'

    # Persist to BigQuery
    # WriteToBigQuery accepts the data as list of JSON objects
