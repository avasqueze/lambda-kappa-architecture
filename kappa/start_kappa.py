
import time
import subprocess
import threading


def run_command_in_docker(container_name, command):
    """
    Ejecuta un comando para un contenedor.

    :param container_name: Nombre del Contenedor
    :param command: Comando que necesita ser procesado por el contenedor
    """
    try:
        docker_command = ["docker", "exec", container_name] + command
        print(f"Ejecutando comando en el contenedor {container_name}: {' '.join(docker_command)}")
        subprocess.run(docker_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"El comando '{' '.join(command)}' falló con el error: {e}")


def run_commands_in_parallel(commands):
    """
    Ejecuta todos los comandos en paralelo, para iniciar cada proceso en paralelo.

    :param commands: Comandos con los nombres de los Contenedores
    """

    threads = []
    for container_name, command in commands:
        time.sleep(10)
        thread = threading.Thread(target=run_command_in_docker, args=(container_name, command))
        threads.append(thread)
        thread.start()
        print(f"El contenedor {container_name} inició el comando {command}")

    for thread in threads:
        thread.join()


def main():
    """
    Define y ejecuta los comandos.
    """

    commands = [
        ("spark-master", ["bash", "-c",
                          "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/kafka_to_spark_to_cassandra.py broker:29092"]),
        ("broker",
         ["bash", "-c", "python3 src/kafka_to_cassandra.py & python3 src/snack_to_kafka.py && tail -f /dev/null"])
    ]

    run_commands_in_parallel(commands)


if __name__ == "__main__":
    main()
