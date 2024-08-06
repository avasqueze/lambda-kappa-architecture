from helper_docker import run_command_in_docker
import time
import subprocess
import threading


def run_command_in_docker(container_name, command):
    try:
        # Construct the docker command
        docker_command = ["docker", "exec", container_name] + command
        print(f"Running command in container {container_name}: {' '.join(docker_command)}")
        subprocess.run(docker_command, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command '{' '.join(command)}' failed with error: {e}")


def run_commands_in_parallel(commands):
    threads = []
    for container_name, command in commands:
        time.sleep(10)
        thread = threading.Thread(target=run_command_in_docker, args=(container_name, command))
        threads.append(thread)
        thread.start()
        print(f"{container_name} started command {command}")

    for thread in threads:
        thread.join()


def main():
    commands = [
        ("lambda-namenode-1", ["bash", "rewrite_mapred_site_namenode.sh"]),
        ("lambda-namenode-1", ["bash", "-c", "python3 src/kafka_to_hadoop.py"]),
        ("spark-master", ["bash", "-c",
                          "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 src/kafka_to_spark_to_cassandra.py broker:29092"]),
        ("lambda-namenode-1",
         ["bash", "-c", "python3 src/hadoop_to_cassandra.py -r hadoop hdfs:///data.json > results.txt"]),
        ("broker",
         ["bash", "-c", "python3 src/kafka_to_cassandra.py & python3 src/snack_to_kafka.py && tail -f /dev/null"]),
    ]

    run_commands_in_parallel(commands)


if __name__ == "__main__":
    main()
