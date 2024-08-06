import docker

def run_command_in_docker(container_name, command):
    client = docker.from_env()
    container = client.containers.get(container_name)
    exec_result = container.exec_run(command)
    output = exec_result.output.decode().strip()
    exit_code = exec_result.exit_code
    print(output, exit_code)
    return None
