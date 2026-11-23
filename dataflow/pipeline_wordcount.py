import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    # Configuración del pipeline
    options = PipelineOptions(
        runner="DataflowRunner",  # Cambiar a DataflowRunner para GCP Cambiar a DirectRunner para Local
        project="gcp-ingeniero-de-datos-04-2",
        region="us-east1",
        temp_location="gs://gcs-bucket-pipeline-2/temp"
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Leer archivo" >> beam.io.ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
            | "Separar palabras" >> beam.FlatMap(lambda line: line.split())
            | "Contar palabras" >> beam.combiners.Count.PerElement()
            | "Guardar resultados" >> beam.io.WriteToText("gs://gcs-bucket-pipeline-2/output/wordcount")
        )
    print("Pipeline ejecutado exitosamente.")

if __name__ == "__main__":
    run()