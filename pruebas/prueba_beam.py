import apache_beam as beam

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Crear numeros" >> beam.Create([1, 2, 3])
        | "Multiplicar por 10" >> beam.Map(lambda x: x * 10)
        | "Mostrar" >> beam.Map(print)
    )
